package schema

import (
	"fmt"
	"regexp/syntax"
	"slices"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"
)

// maxPatternThreads bounds the program instructions Go's regexp may
// visit for one byte of the string. Its matcher is linear in the input,
// but on every byte it advances every live partial match and follows
// every instruction reachable from them without consuming input
// (alternations, captures, empty-width assertions such as \B), and an
// unanchored pattern restarts at every position: "a.{1000}b" keeps a
// thousand partial matches alive and took 5 s on a 1 MiB string, and
// "(?:\B){1000}x" keeps one but walks a thousand assertions per byte,
// 3.4 s. One instruction visit costs a few nanoseconds per byte, so 32
// keeps a 1 MiB string well under a second while admitting anchored
// patterns of any length and the usual unanchored ones (an unanchored
// UUID visits at most a dozen).
const maxPatternThreads = 32

// Budgets of the pattern analysis. patternExploreBudget bounds the
// exact exploration of one pattern (instruction visits); past it the
// cheaper over-approximation decides. patternApproxSteps bounds that
// one, which fails closed when it has not settled.
const (
	patternExploreBudget = 1 << 18
	patternApproxSteps   = 4096
	patternClassBudget   = 1 << 20
)

// patternThreads returns an upper bound on the program instructions
// Go's regexp visits for one byte when matching pattern against any
// string, which is what each byte of the string costs. A pattern the
// regexp syntax refuses yields 0: the compiler reports it.
//
// The matcher (regexp's NFA, used for inputs too long to backtrack)
// holds at most one thread per consuming instruction, adds a thread at
// the start instruction at every position unless the pattern is
// anchored with ^, and keeps a thread only while the input matches. On
// each byte it visits every instruction in the closure of the threads
// it keeps (each at most once): the consuming ones and every
// alternation, capture, no-op and empty-width assertion on the way to
// them. The exact bound is the largest such closure a subset
// construction over the input's character classes reaches; when that
// exploration is too large, the bound assumes every character matches
// every instruction, which can only overcount.
func patternThreads(pattern string) int {
	re, err := syntax.Parse(pattern, syntax.Perl)
	if err != nil {
		return 0
	}
	prog, err := syntax.Compile(re.Simplify())
	if err != nil {
		return 0
	}
	a := &patternNFA{prog: prog, anchored: prog.StartCond()&syntax.EmptyBeginText != 0}
	if n, ok := a.exact(); ok {
		return n
	}
	return a.overApprox()
}

type patternNFA struct {
	prog     *syntax.Prog
	anchored bool
	work     int
}

func consumes(op syntax.InstOp) bool {
	switch op {
	case syntax.InstRune, syntax.InstRune1, syntax.InstRuneAny, syntax.InstRuneAnyNotNL:
		return true
	}
	return false
}

func (a *patternNFA) matches(inst *syntax.Inst, r rune) bool {
	switch inst.Op {
	case syntax.InstRune1:
		return inst.Rune[0] == r
	case syntax.InstRune:
		return inst.MatchRune(r)
	case syntax.InstRuneAny:
		return true
	case syntax.InstRuneAnyNotNL:
		return r != '\n'
	}
	return false
}

// closure follows the non-consuming instructions from pcs and returns
// the consuming ones reached, sorted, and how many instructions it
// visited, consuming or not: what the matcher pays for this step.
// Empty-width assertions (^, $, \b) are assumed to hold, which can
// only add threads.
func (a *patternNFA) closure(pcs []uint32) ([]uint32, int) {
	seen := map[uint32]bool{}
	var out []uint32
	stack := slices.Clone(pcs)
	for len(stack) > 0 {
		pc := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if seen[pc] {
			continue
		}
		seen[pc] = true
		a.work++
		inst := &a.prog.Inst[pc]
		switch {
		case consumes(inst.Op):
			out = append(out, pc)
		case inst.Op == syntax.InstAlt || inst.Op == syntax.InstAltMatch:
			stack = append(stack, inst.Out, inst.Arg)
		case inst.Op == syntax.InstCapture || inst.Op == syntax.InstNop || inst.Op == syntax.InstEmptyWidth:
			stack = append(stack, inst.Out)
		}
	}
	slices.Sort(out)
	return out, len(seen)
}

func threadKey(pcs []uint32) string {
	var b strings.Builder
	for _, pc := range pcs {
		b.WriteString(strconv.FormatUint(uint64(pc), 36))
		b.WriteByte(',')
	}
	return b.String()
}

// classes returns one representative rune for every distinct way the
// program's consuming instructions can react to a character, or nil
// when the partition is too large. The compiler keeps case folding
// only on single-rune instructions (a folded class is expanded into
// its ranges), so a folded rune contributes its simple-fold orbit.
func (a *patternNFA) classes() []rune {
	var consumers []int
	points := []rune{0}
	for i := range a.prog.Inst {
		inst := &a.prog.Inst[i]
		if !consumes(inst.Op) {
			continue
		}
		consumers = append(consumers, i)
		switch inst.Op {
		case syntax.InstRune:
			if len(inst.Rune) == 1 {
				// A single rune, case-folded when the flag is set: it
				// matches its whole simple-fold orbit.
				r := inst.Rune[0]
				points = append(points, r, r+1)
				if syntax.Flags(inst.Arg)&syntax.FoldCase != 0 {
					for f := unicode.SimpleFold(r); f != r; f = unicode.SimpleFold(f) {
						points = append(points, f, f+1)
					}
				}
				continue
			}
			for j := 0; j+1 < len(inst.Rune); j += 2 {
				points = append(points, inst.Rune[j], inst.Rune[j+1]+1)
			}
		case syntax.InstRune1:
			points = append(points, inst.Rune[0], inst.Rune[0]+1)
		case syntax.InstRuneAnyNotNL:
			points = append(points, '\n', '\n'+1)
		}
	}
	slices.Sort(points)
	points = slices.Compact(points)
	if len(points)*len(consumers) > patternClassBudget {
		return nil
	}
	seen := map[string]bool{}
	var reps []rune
	sig := make([]byte, (len(consumers)+7)/8)
	for i, p := range points {
		end := rune(unicode.MaxRune + 1)
		if i+1 < len(points) {
			end = points[i+1]
		}
		// Every rune in [p, end) reacts alike. Surrogates never occur
		// in a decoded string, so an interval starting on one is
		// represented by its first rune past them, if it has one.
		if p >= 0xD800 && p <= 0xDFFF {
			p = 0xE000
		}
		if p >= end || p > unicode.MaxRune {
			continue
		}
		clear(sig)
		for k, pc := range consumers {
			if a.matches(&a.prog.Inst[pc], p) {
				sig[k/8] |= 1 << (k % 8)
			}
		}
		if key := string(sig); !seen[key] {
			seen[key] = true
			reps = append(reps, p)
		}
	}
	return reps
}

// step returns the threads alive after one character of class r and
// the instructions visited to find them.
func (a *patternNFA) step(threads []uint32, r rune) ([]uint32, int) {
	var next []uint32
	for _, pc := range threads {
		inst := &a.prog.Inst[pc]
		if a.matches(inst, r) {
			next = append(next, inst.Out)
		}
	}
	if !a.anchored {
		next = append(next, uint32(a.prog.Start))
	}
	return a.closure(next)
}

// exact explores the subset construction over the character classes
// and returns the largest thread set it reaches. ok is false when the
// exploration was not possible or ran over budget.
func (a *patternNFA) exact() (int, bool) {
	reps := a.classes()
	if reps == nil {
		return 0, false
	}
	start, most := a.closure([]uint32{uint32(a.prog.Start)})
	seen := map[string]bool{threadKey(start): true}
	queue := [][]uint32{start}
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		for _, r := range reps {
			next, visited := a.step(cur, r)
			most = max(most, visited)
			if most > maxPatternThreads {
				return most, true
			}
			if a.work > patternExploreBudget {
				return 0, false
			}
			if k := threadKey(next); !seen[k] {
				seen[k] = true
				queue = append(queue, next)
			}
		}
	}
	return most, true
}

// overApprox follows the thread set when every character matches every
// consuming instruction, which contains the real set at every step.
// It fails closed (reports a bound past the limit) when the sequence
// has not settled within patternApproxSteps.
func (a *patternNFA) overApprox() int {
	cur, most := a.closure([]uint32{uint32(a.prog.Start)})
	seen := map[string]bool{threadKey(cur): true}
	for range patternApproxSteps {
		var next []uint32
		for _, pc := range cur {
			next = append(next, a.prog.Inst[pc].Out)
		}
		if !a.anchored {
			next = append(next, uint32(a.prog.Start))
		}
		var visited int
		cur, visited = a.closure(next)
		most = max(most, visited)
		if most > maxPatternThreads {
			return most
		}
		k := threadKey(cur)
		if seen[k] {
			return most
		}
		seen[k] = true
	}
	return maxPatternThreads + 1
}

// checkPatternCosts refuses, at registration, any pattern the
// validator can apply (the pattern keyword or a patternProperties key
// of any subschema the compiler resolved, propertyNames included) that
// can make the matcher visit more than maxPatternThreads instructions
// per byte.
func checkPatternCosts(g *compiledGraph) error {
	checked := map[string]bool{}
	check := func(pattern, where string) error {
		if checked[pattern] {
			return nil
		}
		checked[pattern] = true
		if n := patternThreads(pattern); n > maxPatternThreads {
			shown := pattern
			if len(shown) > 64 {
				cut := 61
				for cut > 0 && !utf8.RuneStart(shown[cut]) {
					cut--
				}
				shown = shown[:cut] + "..."
			}
			return &costError{msg: fmt.Sprintf("%s: pattern %q can make matching pay for more than %d steps on every byte of the string; anchor it with ^ or shorten its repetitions",
				where, shown, maxPatternThreads)}
		}
		return nil
	}
	for id, s := range g.schemas {
		where := "at #" + g.nodes[id].path
		if s.Pattern != nil {
			if err := check(s.Pattern.String(), where+"/pattern"); err != nil {
				return err
			}
		}
		for _, re := range patternKeys(s.PatternProperties) {
			if err := check(re.String(), where+"/patternProperties"); err != nil {
				return err
			}
		}
	}
	return nil
}
