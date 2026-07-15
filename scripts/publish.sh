#!/usr/bin/env bash
#
# Publica @modulocker/uhura-nestjs no Verdaccio interno da Modulocker.
#   https://verdaccio.modulocker.app.br (escopo @modulocker/* privado, publish: publisher)
#
# O token de auth fica em .npm_secret (git-ignored), no formato npmrc:
#   //verdaccio.modulocker.app.br/:_authToken=<token do usuario publisher>
#
# Para obter um token (JWT, expira em 365d):
#   npm login --registry https://verdaccio.modulocker.app.br
#
set -euo pipefail

cd "$(dirname "$0")/.."

REGISTRY="https://verdaccio.modulocker.app.br"
SECRET="${NPM_SECRET_FILE:-.npm_secret}"

if [[ ! -f "$SECRET" ]]; then
  echo "erro: arquivo de token '$SECRET' não encontrado." >&2
  echo "crie-o com: echo '//verdaccio.modulocker.app.br/:_authToken=SEU_TOKEN' > $SECRET" >&2
  exit 1
fi

echo ">> verificando registry…"
npm ping --registry "$REGISTRY" --userconfig "$SECRET"

echo ">> limpando e buildando…"
npm run clean
npm run build

echo ">> publicando em $REGISTRY…"
npm publish --registry "$REGISTRY" --userconfig "$SECRET" "$@"

echo ">> ok."
