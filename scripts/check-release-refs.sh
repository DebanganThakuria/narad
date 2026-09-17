#!/usr/bin/env bash
#
# Guards against the staleness bug this repository keeps hitting: a
# pinned image tag in the docs still naming an old version months after
# a new one shipped. A reader who finds `narad:v0.2.0-beta.3` on the
# deployment page reasonably concludes nobody is home.
#
# The rule: every pinned `ghcr.io/debanganthakuria/narad:vX.Y.Z`
# reference in tracked documentation must name the newest stable release
# tag. Floating references (`:latest`, `:dev`, `:sha-...`) are the right
# answer in many places and are ignored here.
#
# Release order this implies: update the docs, land that, then tag. See
# the "Releasing" section of CONTRIBUTING.md.
set -euo pipefail

cd "$(dirname "$0")/.."

readonly IMAGE="ghcr.io/debanganthakuria/narad"
# The pre-release suffix is part of the match on purpose. Without it the
# pattern has no right anchor, so a docs pin of v1.2.0-rc.1 matched only
# as far as "v1.2.0" and compared equal to a v1.2.0 release: the one
# thing the comment below says must never happen would have passed.
#
# The suffix is spelled as dot-separated alphanumeric groups rather than
# "anything that is not whitespace", so ordinary punctuation after a
# reference is not swallowed into the version. A ref that ends a
# sentence, or sits inside a Markdown link, would otherwise read as
# "v3.0.1." or "v3.0.1](https://..." and fail against a correct v3.0.1.
readonly PATTERN="${IMAGE}:v[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z-]+(\.[0-9A-Za-z-]+)*)?"

# Stable tags only: a pre-release (v1.0.0-rc.1) must never become the
# version the docs tell people to run.
latest="$(git tag --list 'v*' --sort=-v:refname \
	| grep -E '^v[0-9]+\.[0-9]+\.[0-9]+$' \
	| head -1 || true)"

if [[ -z "${latest}" ]]; then
	echo "check-release-refs: no release tags visible, skipping."
	echo "  (a shallow CI checkout needs fetch-depth: 0 for this check to run)"
	exit 0
fi

hits="$(grep -rnE "${PATTERN}" README.md docs/ 2>/dev/null || true)"

if [[ -z "${hits}" ]]; then
	echo "check-release-refs: no pinned image references found."
	exit 0
fi

# A while-read loop rather than mapfile: macOS still ships bash 3.2 and
# this script runs from `make check` on developer laptops too.
failed=0
while IFS= read -r hit; do
	[[ -n "${hit}" ]] || continue
	# hit is path:line:content. grep -rnE reports one line per source
	# line, not per match, so a line carrying two pins has to be checked
	# twice: taking only the first let a stale second pin through.
	location="$(printf '%s' "${hit}" | cut -d: -f1,2)"
	while IFS= read -r found; do
		[[ -n "${found}" ]] || continue
		found="${found#"${IMAGE}":}"
		if [[ "${found}" != "${latest}" ]]; then
			echo "${location}: pins ${found}, newest release is ${latest}"
			failed=1
		fi
	done <<INNER
$(printf '%s' "${hit}" | grep -oE "${PATTERN}" || true)
INNER
done <<EOF
${hits}
EOF

if [[ ${failed} -ne 0 ]]; then
	cat <<MSG

check-release-refs: stale pinned version(s) above.
Fix: point them at ${latest}, or use :latest where a floating tag is correct.
MSG
	exit 1
fi

echo "check-release-refs: all pinned image references name ${latest}."
