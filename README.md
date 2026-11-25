# aop-cc

O núcleo comum da plataforma do testbed de Codificação de Aplicações da TV 3.0, o lado do receptor da
camada de transporte. Ele adquire a sinalização da emissora simulada e a distribui na mensageria,
entrega os objetos de radiodifusão pela comporta, guarda as aplicações no Application Context Cache
e decide, pela BALD, que aplicação fica na camada de gráficos.

Este repositório é submódulo do [TV30](https://github.com/multisens/TV30) e sobe pelo
`docker-compose.yml` da raiz dele.

**Documentação:** <https://multisens.com.br/TV30/aop-cc/>

## Desenvolver e testar

- Variáveis de ambiente e pacotes do código:
  [aop-cc](https://multisens.com.br/TV30/aop-cc/#desenvolver-e-testar).
- Testes: `go vet ./...` e `go test ./...`; as suítes com a stack de pé estão em
  [Como testar](https://multisens.com.br/TV30/como-testar/).

## Licença

Apache-2.0; veja o `LICENSE`.
