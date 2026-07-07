#!/usr/bin/env bash
#
# Publica @marcosaquino/uhura-nestjs no npmjs.com (público).
# O token de auth fica em .npm_secret (git-ignored), no formato npmrc:
#   //registry.npmjs.org/:_authToken=npm_xxxxxxxx
#
set -euo pipefail

cd "$(dirname "$0")/.."

SECRET="${NPM_SECRET_FILE:-.npm_secret}"

if [[ ! -f "$SECRET" ]]; then
  echo "erro: arquivo de token '$SECRET' não encontrado." >&2
  echo "crie-o com: echo '//registry.npmjs.org/:_authToken=SEU_TOKEN' > $SECRET" >&2
  exit 1
fi

echo ">> limpando e buildando…"
npm run clean
npm run build

echo ">> publicando (público)…"
npm publish --access public --userconfig "$SECRET" "$@"

echo ">> ok."
