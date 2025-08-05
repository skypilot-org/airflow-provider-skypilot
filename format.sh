#!/usr/bin/env bash

set -eo pipefail

builtin cd "$(dirname "${BASH_SOURCE:-$0}")"
ROOT="$(git rev-parse --show-toplevel)"
builtin cd "$ROOT" || exit 1

YAPF_FLAGS=(
    '--recursive'
    '--parallel'
)

# Format specified files
format() {
    yapf --in-place "${YAPF_FLAGS[@]}" "$@"
}

# Format files that differ from main branch
format_changed() {
    MERGEBASE="$(git merge-base origin/main HEAD)"

    if ! git diff --diff-filter=ACM --quiet --exit-code "$MERGEBASE" -- '*.py' '*.pyi' &>/dev/null; then
        git diff --name-only --diff-filter=ACM "$MERGEBASE" -- '*.py' '*.pyi' | \
            tr '\n' '\0' | xargs -P 5 -0 \
            yapf --in-place "${YAPF_FLAGS[@]}"
    fi
}

# Format all files
format_all() {
    yapf --in-place "${YAPF_FLAGS[@]}" .
}

if [[ "$1" == '--files' ]]; then
   format "${@:2}"
elif [[ "$1" == '--all' ]]; then
   format_all
else
   format_changed
fi

isort .

if ! git diff --quiet &>/dev/null; then
    echo 'Reformatted files. Please review and stage the changes.'
    echo 'Changes not staged for commit:'
    echo
    git --no-pager diff --name-only

    exit 1
fi
