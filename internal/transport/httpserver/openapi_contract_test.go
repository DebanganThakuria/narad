package httpserver

// The OpenAPI contract test: docs/reference/openapi.yaml against the code.
// That file is the one spec: the docs site publishes it, and
// scripts/gen_http_api.py renders the HTTP API reference page from it.
//
// TestOpenAPIContractRoutes fails when router.go registers a route the
// spec does not document, or the spec documents one the router does not
// register.
//
// TestOpenAPIContractStatusCodes fails when the spec documents a status
// code an operation can never answer, and when a handler can answer a
// 2xx code the spec leaves out. What an operation can answer is worked
// out from the source without running it: the status constants
// (http.StatusXxx, handlers.StatusClientClosedRequest) that its handler,
// and every function and method the handler calls in the httpserver
// packages, writes or returns, plus the codes of the middleware in front
// of the route. That is checkable only for handlers in the httpserver
// tree: /metrics is served by the Prometheus library, so only its
// middleware codes are checked. A response marked x-narad-origin:
// forwarding must be a code the cluster package (internal/cluster)
// writes, since the node that forwards a request writes it.
//
// The spec is read with a small outline reader instead of a YAML library
// (the module has none): the test needs only mapping keys, scalar values
// and indentation, and the spec keeps to two-space block style.

import (
	"bufio"
	"bytes"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/debanganthakuria/narad/internal/transport/httpserver/handlers"
)

const (
	contractRepoRoot = "../../.."
	contractSpecPath = contractRepoRoot + "/docs/reference/openapi.yaml"
	contractModule   = "github.com/debanganthakuria/narad/"
	// contractTree is the import path of the packages the status
	// analysis reads: this package and the handlers under it.
	contractTree       = contractModule + "internal/transport/httpserver"
	contractClusterDir = contractRepoRoot + "/internal/cluster"
)

// contractExternalHandlers are the routes whose handler lives outside
// the httpserver tree, so only their middleware codes are checked.
var contractExternalHandlers = map[string]bool{
	"GET /metrics": true,
}

func TestOpenAPIContractRoutes(t *testing.T) {
	spec := loadContractSpec(t)
	routes := loadRouterRoutes(t)

	for _, key := range sortedKeys(routes) {
		if _, ok := spec.ops[key]; !ok {
			t.Errorf("router.go registers %s, but docs/reference/openapi.yaml does not document it", key)
		}
	}
	for _, key := range sortedKeys(spec.ops) {
		if _, ok := routes[key]; !ok {
			op := spec.ops[key]
			t.Errorf("docs/reference/openapi.yaml documents %s (%s, line %d), but router.go does not register it",
				key, op.id, op.line)
		}
	}
}

func TestOpenAPIContractStatusCodes(t *testing.T) {
	spec := loadContractSpec(t)
	routes := loadRouterRoutes(t)
	a := newStatusAnalysis(t)
	mw := a.middlewareCodes(t)
	forwarding := a.forwardingCodes(t)
	exempt := authExemptPaths(false)

	for _, key := range sortedKeys(spec.ops) {
		op := spec.ops[key]
		route, ok := routes[key]
		if !ok {
			continue // TestOpenAPIContractRoutes reports it
		}
		method, path, _ := strings.Cut(key, " ")
		external := contractExternalHandlers[key]
		if !external && len(route.roots) == 0 {
			t.Errorf("%s: its handler is not in the httpserver packages; if another package serves it, add it to contractExternalHandlers", key)
			continue
		}

		handlerCodes := a.reachableCodes(t, route.roots)
		allowed := union(mw.all, handlerCodes)
		if !exempt[path] {
			allowed = union(allowed, mw.auth)
		}
		if method == http.MethodPost || method == http.MethodPut || method == http.MethodPatch {
			allowed = union(allowed, mw.contentType)
		}

		documented := map[int]bool{}
		for _, r := range op.responses {
			documented[r.code] = true
			switch {
			case r.origin == "forwarding":
				if !forwarding[r.code] {
					t.Errorf("%s (%s): %d at line %d is marked x-narad-origin: forwarding, but internal/cluster never writes it",
						key, op.id, r.code, r.line)
				}
			case external && r.code >= 200 && r.code < 300:
				// The external handler's own answer: not checkable here.
			case !allowed[r.code]:
				t.Errorf("%s (%s): docs/reference/openapi.yaml documents %d at line %d, but neither the handler nor its middleware can answer it",
					key, op.id, r.code, r.line)
			}
		}
		for _, code := range sortedInts(handlerCodes) {
			if code >= 200 && code < 300 && !documented[code] {
				t.Errorf("%s (%s): the handler can answer %d, but docs/reference/openapi.yaml does not document it", key, op.id, code)
			}
		}
	}
}

// ---- the spec ---------------------------------------------------------

type contractSpec struct {
	ops map[string]contractOp // by "METHOD /path"
}

type contractOp struct {
	id        string
	line      int
	responses []contractResponse
}

type contractResponse struct {
	code   int
	origin string // x-narad-origin, after following $ref
	line   int
}

var contractMethods = map[string]bool{"get": true, "put": true, "post": true, "patch": true, "delete": true}

func loadContractSpec(t *testing.T) contractSpec {
	t.Helper()
	src, err := os.ReadFile(contractSpecPath)
	if err != nil {
		t.Fatalf("read the spec: %v", err)
	}
	root, err := parseYAMLOutline(src)
	if err != nil {
		t.Fatalf("read %s: %v", contractSpecPath, err)
	}
	paths := root.child("paths")
	if paths == nil {
		t.Fatalf("%s has no paths", contractSpecPath)
	}

	spec := contractSpec{ops: map[string]contractOp{}}
	ids := map[string]string{}
	for _, p := range paths.children {
		for _, m := range p.children {
			if !contractMethods[m.key] {
				continue
			}
			key := strings.ToUpper(m.key) + " " + p.key
			op := contractOp{line: m.line}
			if id := m.child("operationId"); id != nil {
				op.id = id.value
			}
			switch other, dup := ids[op.id]; {
			case op.id == "":
				t.Errorf("%s (line %d) has no operationId", key, m.line)
			case dup:
				t.Errorf("operationId %s is used by both %s and %s", op.id, other, key)
			}
			ids[op.id] = key

			responses := m.child("responses")
			if responses == nil {
				t.Errorf("%s (line %d) has no responses", key, m.line)
				continue
			}
			for _, r := range responses.children {
				code, err := strconv.Atoi(r.key)
				if err != nil || code < 100 || code > 599 {
					t.Errorf("%s: response %q at line %d is not a status code", key, r.key, r.line)
					continue
				}
				resolved, err := root.resolve(r)
				if err != nil {
					t.Errorf("%s: response %d: %v", key, code, err)
					continue
				}
				resp := contractResponse{code: code, line: r.line}
				if o := resolved.child("x-narad-origin"); o != nil {
					resp.origin = o.value
				}
				op.responses = append(op.responses, resp)
			}
			spec.ops[key] = op
		}
	}
	if len(spec.ops) == 0 {
		t.Fatalf("%s documents no operations; has its layout changed?", contractSpecPath)
	}
	return spec
}

// yamlNode is one mapping key of a YAML document with its inline scalar
// value and the keys nested under it. Sequences and block scalar bodies
// are skipped: the contract needs neither.
type yamlNode struct {
	key      string
	value    string
	line     int
	indent   int
	children []*yamlNode
}

func (n *yamlNode) child(key string) *yamlNode {
	for _, c := range n.children {
		if c.key == key {
			return c
		}
	}
	return nil
}

// resolve follows target's local $ref chain; n is the document root.
func (n *yamlNode) resolve(target *yamlNode) (*yamlNode, error) {
	for range 10 {
		ref := target.child("$ref")
		if ref == nil {
			return target, nil
		}
		path, ok := strings.CutPrefix(ref.value, "#/")
		if !ok {
			return nil, fmt.Errorf("line %d: only local $refs are supported, got %q", ref.line, ref.value)
		}
		next := n
		for _, seg := range strings.Split(path, "/") {
			if next = next.child(seg); next == nil {
				return nil, fmt.Errorf("line %d: $ref %q does not resolve", ref.line, ref.value)
			}
		}
		target = next
	}
	return nil, fmt.Errorf("line %d: $ref chain too long", target.line)
}

// yamlKeyLine matches `key:` or `key: value`, with the key plain,
// "double-quoted" or 'single-quoted'.
var yamlKeyLine = regexp.MustCompile(`^( *)("[^"]*"|'[^']*'|[^\s#'"\-][^:#]*?|-\S[^:#]*?)\s*:(?:\s+(.*))?$`)

// parseYAMLOutline reads the mapping keys of a block-style YAML document.
func parseYAMLOutline(src []byte) (*yamlNode, error) {
	root := &yamlNode{indent: -1}
	stack := []*yamlNode{root}
	// skip, when >= 0, drops lines indented deeper than it: the body of a
	// sequence item, a block scalar, or a continued plain scalar.
	skip := -1
	sc := bufio.NewScanner(bytes.NewReader(src))
	sc.Buffer(make([]byte, 0, 64<<10), 1<<20)
	for lineNo := 1; sc.Scan(); lineNo++ {
		line := sc.Text()
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "#") {
			continue
		}
		body := strings.TrimLeft(line, " ")
		if strings.HasPrefix(body, "\t") {
			return nil, fmt.Errorf("line %d: tab indentation", lineNo)
		}
		indent := len(line) - len(body)
		if skip >= 0 {
			if indent > skip {
				continue
			}
			skip = -1
		}
		if trimmed == "-" || strings.HasPrefix(trimmed, "- ") {
			skip = indent
			continue
		}
		m := yamlKeyLine.FindStringSubmatch(line)
		if m == nil {
			return nil, fmt.Errorf("line %d: expected a mapping key: %q", lineNo, trimmed)
		}
		node := &yamlNode{key: unquoteYAML(m[2]), value: yamlScalar(m[3]), line: lineNo, indent: indent}
		for len(stack) > 1 && stack[len(stack)-1].indent >= indent {
			stack = stack[:len(stack)-1]
		}
		parent := stack[len(stack)-1]
		parent.children = append(parent.children, node)
		stack = append(stack, node)
		if node.value != "" {
			// A key with an inline value has no nested keys.
			skip = indent
		}
	}
	return root, sc.Err()
}

func unquoteYAML(s string) string {
	if len(s) >= 2 && (s[0] == '"' || s[0] == '\'') && s[len(s)-1] == s[0] {
		return s[1 : len(s)-1]
	}
	return s
}

// yamlScalar is an inline value without quotes or a trailing comment.
func yamlScalar(v string) string {
	v = strings.TrimSpace(v)
	if strings.HasPrefix(v, "#") {
		return ""
	}
	if len(v) > 0 && v[0] != '"' && v[0] != '\'' {
		if i := strings.Index(v, " #"); i >= 0 {
			v = strings.TrimSpace(v[:i])
		}
	}
	return unquoteYAML(v)
}

// ---- the router -------------------------------------------------------

type routerRoute struct {
	roots []funcRef // what the handler expression calls in the tree
}

// funcRef names a function (recv empty), a method, or with anyRecv a
// method of whatever type in dir declares it.
type funcRef struct {
	dir     string // package directory, relative to the repo root
	recv    string
	name    string
	anyRecv bool
}

var routePattern = regexp.MustCompile(`^(GET|POST|PUT|PATCH|DELETE) (/\S*)$`)

func loadRouterRoutes(t *testing.T) map[string]routerRoute {
	t.Helper()
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "router.go", nil, parser.SkipObjectResolution)
	if err != nil {
		t.Fatalf("parse router.go: %v", err)
	}
	imports := fileImports(f)
	here := dirOf(contractTree)

	routes := map[string]routerRoute{}
	ast.Inspect(f, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok || len(call.Args) != 2 {
			return true
		}
		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok || (sel.Sel.Name != "Handle" && sel.Sel.Name != "HandleFunc") {
			return true
		}
		if x, ok := sel.X.(*ast.Ident); !ok || x.Name != "mux" {
			return true
		}
		lit, ok := call.Args[0].(*ast.BasicLit)
		if !ok || lit.Kind != token.STRING {
			t.Errorf("%s: a route pattern that is not a string literal, which this test cannot read", fset.Position(call.Pos()))
			return true
		}
		pattern, err := strconv.Unquote(lit.Value)
		if err != nil || !routePattern.MatchString(pattern) {
			t.Errorf("%s: route pattern %s must be \"METHOD /path\"", fset.Position(call.Pos()), lit.Value)
			return true
		}
		if _, dup := routes[pattern]; dup {
			t.Errorf("router.go registers %s twice", pattern)
		}

		// httptopics.Create(h) is a function of a handler package;
		// produceLimit.wrap(...) is a method of a local variable, which
		// can only be of a type of this package.
		var roots []funcRef
		ast.Inspect(call.Args[1], func(n ast.Node) bool {
			c, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			if fun, ok := c.Fun.(*ast.SelectorExpr); ok {
				if x, ok := fun.X.(*ast.Ident); ok {
					switch path, isPkg := imports[x.Name]; {
					case isPkg && strings.HasPrefix(path, contractTree):
						roots = append(roots, funcRef{dir: dirOf(path), name: fun.Sel.Name})
					case !isPkg:
						roots = append(roots, funcRef{dir: here, name: fun.Sel.Name, anyRecv: true})
					}
				}
			}
			return true
		})
		routes[pattern] = routerRoute{roots: roots}
		return true
	})
	if len(routes) == 0 {
		t.Fatal("found no mux.Handle or mux.HandleFunc calls in router.go")
	}
	return routes
}

// ---- the status analysis ----------------------------------------------

type contractPackage struct {
	funcs   map[string]*contractFunc
	methods map[string]map[string]*contractFunc // receiver type, method
	structs map[string]contractStruct
}

type contractFunc struct {
	dir     string
	decl    *ast.FuncDecl
	imports map[string]string // alias -> import path, for its file
}

type contractStruct struct {
	fields  map[string]ast.Expr
	imports map[string]string
}

type statusAnalysis struct {
	pkgs  map[string]*contractPackage // by directory
	names map[string]int              // "StatusNotFound" -> 404
}

func newStatusAnalysis(t *testing.T) *statusAnalysis {
	t.Helper()
	a := &statusAnalysis{pkgs: map[string]*contractPackage{}, names: statusConstantNames()}
	top := filepath.Join(contractRepoRoot, dirOf(contractTree))
	err := filepath.WalkDir(top, func(path string, d os.DirEntry, err error) error {
		if err != nil || !d.IsDir() {
			return err
		}
		rel, err := filepath.Rel(contractRepoRoot, path)
		if err != nil {
			return err
		}
		return a.loadPackage(filepath.ToSlash(rel))
	})
	if err != nil {
		t.Fatalf("read the httpserver packages: %v", err)
	}
	return a
}

func (a *statusAnalysis) loadPackage(dir string) error {
	pkg := &contractPackage{
		funcs:   map[string]*contractFunc{},
		methods: map[string]map[string]*contractFunc{},
		structs: map[string]contractStruct{},
	}
	a.pkgs[dir] = pkg
	return eachGoFile(filepath.Join(contractRepoRoot, dir), func(f *ast.File) {
		imports := fileImports(f)
		for _, d := range f.Decls {
			switch d := d.(type) {
			case *ast.FuncDecl:
				if d.Body == nil {
					continue
				}
				fn := &contractFunc{dir: dir, decl: d, imports: imports}
				if d.Recv == nil {
					pkg.funcs[d.Name.Name] = fn
					continue
				}
				recv := baseTypeName(d.Recv.List[0].Type)
				if pkg.methods[recv] == nil {
					pkg.methods[recv] = map[string]*contractFunc{}
				}
				pkg.methods[recv][d.Name.Name] = fn
			case *ast.GenDecl:
				for _, spec := range d.Specs {
					ts, ok := spec.(*ast.TypeSpec)
					if !ok {
						continue
					}
					st, ok := ts.Type.(*ast.StructType)
					if !ok {
						continue
					}
					s := contractStruct{fields: map[string]ast.Expr{}, imports: imports}
					for _, field := range st.Fields.List {
						for _, name := range field.Names {
							s.fields[name.Name] = field.Type
						}
						if len(field.Names) == 0 { // embedded
							s.fields[baseTypeName(field.Type)] = field.Type
						}
					}
					pkg.structs[ts.Name.Name] = s
				}
			}
		}
	})
}

func (a *statusAnalysis) lookup(ref funcRef) []*contractFunc {
	pkg := a.pkgs[ref.dir]
	if pkg == nil {
		return nil
	}
	switch {
	case ref.anyRecv:
		var out []*contractFunc
		for _, recv := range sortedKeys(pkg.methods) {
			if fn := pkg.methods[recv][ref.name]; fn != nil {
				out = append(out, fn)
			}
		}
		return out
	case ref.recv != "":
		if fn := pkg.methods[ref.recv][ref.name]; fn != nil {
			return []*contractFunc{fn}
		}
	default:
		if fn := pkg.funcs[ref.name]; fn != nil {
			return []*contractFunc{fn}
		}
	}
	return nil
}

// reachableCodes is every status code the functions in roots, and what
// they call in the tree, write or return.
func (a *statusAnalysis) reachableCodes(t *testing.T, roots []funcRef) map[int]bool {
	codes := map[int]bool{}
	seen := map[*contractFunc]bool{}
	var queue []*contractFunc
	for _, r := range roots {
		queue = append(queue, a.lookup(r)...)
	}
	for len(queue) > 0 {
		fn := queue[0]
		queue = queue[1:]
		if seen[fn] {
			continue
		}
		seen[fn] = true
		for _, ref := range a.scanFunc(t, fn, codes) {
			queue = append(queue, a.lookup(ref)...)
		}
	}
	return codes
}

// scanFunc adds the status codes fn writes or returns to codes, and
// returns the functions and methods of the tree it calls.
//
// A status constant that is compared against (status <
// http.StatusInternalServerError, case http.StatusOK:) is a test, not an
// answer, and does not count. A method call resolves when the type of
// its receiver is known: from fn's receiver and parameters, a var
// declaration, a composite literal, the declared result of a tree
// function it was assigned from, or a struct field of a known type.
// Calls through interfaces do not resolve; the codes they carry are
// constants at the call sites that choose them, which are scanned.
func (a *statusAnalysis) scanFunc(t *testing.T, fn *contractFunc, codes map[int]bool) []funcRef {
	vars := map[string]funcRef{} // variable -> its type as {dir, recv}
	bind := func(name string, typ ast.Expr) {
		if ref, ok := a.typeRef(fn.dir, fn.imports, typ); ok {
			vars[name] = ref
		}
	}
	bindFields := func(fl *ast.FieldList) {
		if fl == nil {
			return
		}
		for _, field := range fl.List {
			for _, name := range field.Names {
				bind(name.Name, field.Type)
			}
		}
	}
	bindFields(fn.decl.Recv)
	bindFields(fn.decl.Type.Params)

	// exprType is the tree type of x, y.f or y.f.g when it is known.
	var exprType func(ast.Expr) (funcRef, bool)
	exprType = func(e ast.Expr) (funcRef, bool) {
		switch e := e.(type) {
		case *ast.Ident:
			ref, ok := vars[e.Name]
			return ref, ok
		case *ast.SelectorExpr:
			owner, ok := exprType(e.X)
			if !ok {
				return funcRef{}, false
			}
			st, ok := a.pkgs[owner.dir].structs[owner.recv]
			if !ok {
				return funcRef{}, false
			}
			field, ok := st.fields[e.Sel.Name]
			if !ok {
				return funcRef{}, false
			}
			return a.typeRef(owner.dir, st.imports, field)
		}
		return funcRef{}, false
	}

	var calls []funcRef
	var stack []ast.Node
	ast.Inspect(fn.decl.Body, func(n ast.Node) bool {
		if n == nil {
			stack = stack[:len(stack)-1]
			return true
		}
		var parent ast.Node
		if len(stack) > 0 {
			parent = stack[len(stack)-1]
		}
		stack = append(stack, n)

		switch n := n.(type) {
		case *ast.FuncLit:
			bindFields(n.Type.Params)
		case *ast.ValueSpec:
			if n.Type != nil {
				for _, name := range n.Names {
					bind(name.Name, n.Type)
				}
			}
		case *ast.AssignStmt:
			// c := begin(...): a variable takes the declared result
			// type of the tree function that made it.
			if len(n.Rhs) == 1 {
				for i, typ := range a.resultTypes(fn, n.Rhs[0]) {
					if i >= len(n.Lhs) {
						break
					}
					if id, ok := n.Lhs[i].(*ast.Ident); ok && typ.recv != "" {
						vars[id.Name] = typ
					}
				}
			}
			for i, rhs := range n.Rhs {
				if i >= len(n.Lhs) {
					break
				}
				id, ok := n.Lhs[i].(*ast.Ident)
				if !ok {
					continue
				}
				if u, ok := rhs.(*ast.UnaryExpr); ok && u.Op == token.AND {
					rhs = u.X
				}
				if lit, ok := rhs.(*ast.CompositeLit); ok && lit.Type != nil {
					bind(id.Name, lit.Type)
				}
			}
		case *ast.CallExpr:
			switch fun := n.Fun.(type) {
			case *ast.Ident:
				calls = append(calls, funcRef{dir: fn.dir, name: fun.Name})
			case *ast.SelectorExpr:
				if x, ok := fun.X.(*ast.Ident); ok {
					if path, isPkg := fn.imports[x.Name]; isPkg {
						if strings.HasPrefix(path, contractTree) {
							calls = append(calls, funcRef{dir: dirOf(path), name: fun.Sel.Name})
						}
						break
					}
				}
				if typ, ok := exprType(fun.X); ok {
					calls = append(calls, funcRef{dir: typ.dir, recv: typ.recv, name: fun.Sel.Name})
				}
			}
		}

		if code, ok := a.statusConstant(t, fn.dir, fn.imports, n); ok && !isComparison(parent) {
			codes[code] = true
		}
		return true
	})
	return calls
}

// resultTypes is the tree type of each result of the function call e
// makes, when e calls a function of the tree (f(...) or pkg.F(...)); a
// result whose type is not a tree type has a zero funcRef.
func (a *statusAnalysis) resultTypes(fn *contractFunc, e ast.Expr) []funcRef {
	call, ok := e.(*ast.CallExpr)
	if !ok {
		return nil
	}
	var ref funcRef
	switch fun := call.Fun.(type) {
	case *ast.Ident:
		ref = funcRef{dir: fn.dir, name: fun.Name}
	case *ast.SelectorExpr:
		x, ok := fun.X.(*ast.Ident)
		if !ok {
			return nil
		}
		path, isPkg := fn.imports[x.Name]
		if !isPkg || !strings.HasPrefix(path, contractTree) {
			return nil
		}
		ref = funcRef{dir: dirOf(path), name: fun.Sel.Name}
	default:
		return nil
	}
	callees := a.lookup(ref)
	if len(callees) != 1 || callees[0].decl.Type.Results == nil {
		return nil
	}
	callee := callees[0]
	var out []funcRef
	for _, field := range callee.decl.Type.Results.List {
		typ, _ := a.typeRef(callee.dir, callee.imports, field.Type)
		for range max(len(field.Names), 1) {
			out = append(out, typ)
		}
	}
	return out
}

// typeRef names the tree type typ denotes (T, *T, pkg.T or *pkg.T), as
// written in a file of dir with the given imports.
func (a *statusAnalysis) typeRef(dir string, imports map[string]string, typ ast.Expr) (funcRef, bool) {
	if star, ok := typ.(*ast.StarExpr); ok {
		typ = star.X
	}
	switch typ := typ.(type) {
	case *ast.Ident:
		return funcRef{dir: dir, recv: typ.Name}, true
	case *ast.SelectorExpr:
		x, ok := typ.X.(*ast.Ident)
		if !ok {
			return funcRef{}, false
		}
		if path, isPkg := imports[x.Name]; isPkg && strings.HasPrefix(path, contractTree) {
			return funcRef{dir: dirOf(path), recv: typ.Sel.Name}, true
		}
	}
	return funcRef{}, false
}

// statusConstant reports the code n names when n is http.StatusXxx or
// handlers.StatusClientClosedRequest (unqualified inside handlers).
func (a *statusAnalysis) statusConstant(t *testing.T, dir string, imports map[string]string, n ast.Node) (int, bool) {
	const closed = "StatusClientClosedRequest"
	switch n := n.(type) {
	case *ast.SelectorExpr:
		x, ok := n.X.(*ast.Ident)
		if !ok {
			return 0, false
		}
		path := imports[x.Name]
		switch {
		case path == contractTree+"/handlers" && n.Sel.Name == closed:
			return handlers.StatusClientClosedRequest, true
		case path != "net/http" || !strings.HasPrefix(n.Sel.Name, "Status") || n.Sel.Name == "StatusText":
			return 0, false
		}
		code, ok := a.names[n.Sel.Name]
		if !ok {
			t.Fatalf("%s uses http.%s, which statusConstantNames does not map to a code", dir, n.Sel.Name)
		}
		return code, true
	case *ast.Ident:
		// The Sel of handlers.StatusClientClosedRequest is an Ident too;
		// only the package's own unqualified uses count here.
		if n.Name == closed && dir == dirOf(contractTree+"/handlers") {
			return handlers.StatusClientClosedRequest, true
		}
	}
	return 0, false
}

func isComparison(n ast.Node) bool {
	switch n := n.(type) {
	case *ast.BinaryExpr:
		switch n.Op {
		case token.EQL, token.NEQ, token.LSS, token.GTR, token.LEQ, token.GEQ:
			return true
		}
	case *ast.CaseClause:
		return true
	}
	return false
}

// contractMiddleware is what the middleware chain can answer, by where
// each middleware applies.
type contractMiddleware struct {
	all         map[int]bool // every route: Recover
	auth        map[int]bool // routes that need credentials: AuthExempting
	contentType map[int]bool // POST, PUT and PATCH: RequireAPIContentType
}

// middlewareCodes reads the chain from router.go. A middleware of this
// package that the test does not know fails it, so a new one cannot
// widen what every route may document without saying where it applies.
func (a *statusAnalysis) middlewareCodes(t *testing.T) contractMiddleware {
	t.Helper()
	f, err := parser.ParseFile(token.NewFileSet(), "router.go", nil, parser.SkipObjectResolution)
	if err != nil {
		t.Fatalf("parse router.go: %v", err)
	}
	chained := map[string]bool{}
	ast.Inspect(f, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		if id, ok := call.Fun.(*ast.Ident); !ok || id.Name != "Chain" {
			return true
		}
		for _, arg := range call.Args {
			if c, ok := arg.(*ast.CallExpr); ok {
				if id, ok := c.Fun.(*ast.Ident); ok {
					chained[id.Name] = true
				}
			}
		}
		return false
	})

	here := dirOf(contractTree)
	codesOf := func(name string) map[int]bool {
		if !chained[name] {
			t.Errorf("router.go no longer chains %s; update middlewareCodes", name)
		}
		delete(chained, name)
		return a.reachableCodes(t, []funcRef{{dir: here, name: name}})
	}
	mw := contractMiddleware{
		all:         codesOf("Recover"),
		auth:        codesOf("AuthExempting"),
		contentType: codesOf("RequireAPIContentType"),
	}
	for _, name := range sortedKeys(chained) {
		t.Errorf("router.go chains %s, which middlewareCodes does not know; say where it applies", name)
	}
	return mw
}

// forwardingCodes is every status code the cluster package writes or
// returns: the answers of a node that forwarded a request and could not
// reach the owner or the leader.
func (a *statusAnalysis) forwardingCodes(t *testing.T) map[int]bool {
	t.Helper()
	codes := map[int]bool{}
	err := eachGoFile(contractClusterDir, func(f *ast.File) {
		imports := fileImports(f)
		var stack []ast.Node
		ast.Inspect(f, func(n ast.Node) bool {
			if n == nil {
				stack = stack[:len(stack)-1]
				return true
			}
			var parent ast.Node
			if len(stack) > 0 {
				parent = stack[len(stack)-1]
			}
			stack = append(stack, n)
			if code, ok := a.statusConstant(t, "internal/cluster", imports, n); ok && !isComparison(parent) {
				codes[code] = true
			}
			return true
		})
	})
	if err != nil {
		t.Fatalf("read %s: %v", contractClusterDir, err)
	}
	return codes
}

// statusConstantNames maps each net/http status constant name to its
// code by way of http.StatusText: "Request Entity Too Large" is
// StatusRequestEntityTooLarge.
func statusConstantNames() map[string]int {
	names := map[string]int{}
	strip := strings.NewReplacer(" ", "", "-", "", "'", "")
	for code := 100; code < 600; code++ {
		if text := http.StatusText(code); text != "" {
			names["Status"+strip.Replace(text)] = code
		}
	}
	return names
}

// ---- helpers ----------------------------------------------------------

// eachGoFile parses the non-test Go files of dir.
func eachGoFile(dir string, fn func(*ast.File)) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	fset := token.NewFileSet()
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, filepath.Join(dir, name), nil, parser.SkipObjectResolution)
		if err != nil {
			return err
		}
		fn(f)
	}
	return nil
}

func fileImports(f *ast.File) map[string]string {
	out := map[string]string{}
	for _, imp := range f.Imports {
		path, err := strconv.Unquote(imp.Path.Value)
		if err != nil {
			continue
		}
		name := path[strings.LastIndex(path, "/")+1:]
		if imp.Name != nil {
			name = imp.Name.Name
		}
		out[name] = path
	}
	return out
}

func dirOf(importPath string) string {
	return strings.TrimPrefix(importPath, contractModule)
}

func baseTypeName(e ast.Expr) string {
	switch e := e.(type) {
	case *ast.StarExpr:
		return baseTypeName(e.X)
	case *ast.Ident:
		return e.Name
	case *ast.SelectorExpr:
		return e.Sel.Name
	case *ast.IndexExpr:
		return baseTypeName(e.X)
	case *ast.IndexListExpr:
		return baseTypeName(e.X)
	}
	return ""
}

func union(sets ...map[int]bool) map[int]bool {
	out := map[int]bool{}
	for _, s := range sets {
		for k, v := range s {
			if v {
				out[k] = true
			}
		}
	}
	return out
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func sortedInts(m map[int]bool) []int {
	out := make([]int, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Ints(out)
	return out
}
