# Relatório executivo: onde o sol não vira painel

**Para:** gestores de política energética e de habitação (MME, ANEEL, governos estaduais, bancos públicos de fomento)
**Autor:** Anderson Huang, RM 565920 · CitizenTech · Global Solution 2026.2
**Base:** 4,73 milhões de sistemas solares (ANEEL, novembro/2026), Atlas Solar (INPE), tarifas (ANEEL), Censo 2022 e PIB 2023 (IBGE)

## Resumo

O Brasil tem 54,6 GW de energia solar distribuída, mas ela está onde há renda, não onde há sol. O Nordeste recebe a maior irradiação do país e cada painel instalado lá economiza 17% a mais por ano do que no Sul. Mesmo assim, a região tem 26,9% da população e só 20,6% da potência instalada, com 30 telhados solares por mil domicílios contra 70 no Centro-Oeste. A renda (PIB per capita) tem correlação de 0,59 com a adoção. A irradiação tem correlação nula (-0,07). Nossa estimativa é de 373 mil telhados residenciais abaixo do esperado no país, o equivalente a R$ 2,25 bilhões por ano de economia que as famílias deixam de ter na conta de luz.

## 1. O que os dados revelam

- **A adoção segue a renda.** Entre os 5.565 municípios analisados, os mais ricos têm muito mais painéis por habitante, independentemente do sol. O sol não explica nada sozinho.
- **O retorno é maior justamente onde a adoção é menor.** Economia anual por kWp instalado: Nordeste R$ 1.216 contra Sul R$ 1.039. A barreira não é a viabilidade do painel, é o acesso ao investimento inicial.
- **O mercado é concentrado.** 10% dos municípios concentram 58,8% da potência. 29% da potência é de fazendas solares de autoconsumo remoto, ou seja, investimento de quem já tem capital.
- **Nas grandes cidades, a moradia vertical também pesa.** Salvador (28% de apartamentos) tem 7 telhados solares por mil domicílios, quando o esperado pelo seu perfil seria 22.

## 2. Situações, regiões e grupos que merecem atenção

1. **Perfil "Muito sol, baixa renda":** 1.676 municípios e 48,4 milhões de pessoas, 88% no Nordeste. Irradiação de 5,48 kWh/m²/dia (a maior), PIB per capita de R$ 15 mil e só 141 W por habitante. Metade da potência instalada nesses municípios é de autoconsumo remoto, isto é, geração cujo crédito vai para consumidores de outros lugares.
2. **Capitais e regiões metropolitanas do Nordeste com maior lacuna:** Salvador (14,4 mil telhados abaixo do esperado), Jaboatão dos Guararapes (4,2 mil), Caucaia (3,5 mil) e Recife.
3. **Perfil "Urbanos de baixa adoção":** 899 municípios e 75,6 milhões de pessoas no litoral e nas regiões metropolitanas do Sudeste e do Norte, com 139 W por habitante.
4. **Estados com menor adoção por habitante:** AM (115 W/hab), RJ (130), SE (147), PB e MA (167).

## 3. Padrões e tendências

- **Ciclo regulatório:** a corrida de 2022 (8,9 GW) antes da Lei 14.300, a queda em 2023 e o recorde de 2024 (10,6 GW). Em 2025 o mercado estabilizou (9,8 GW).
- **O Nordeste é a única região em crescimento:** nos últimos 12 meses estáveis (jun/25 a mai/26), a potência conectada no Nordeste cresceu 12,4%, enquanto o Brasil caiu 2,3%. Paraíba (+54%) e Bahia (+32%) lideram. O mercado do Nordeste está começando a responder, e esse é o momento de política pública ter maior efeito por real investido.
- **Agro e Centro-Oeste lideram por habitante:** MT (872 W/hab) e MS (699) têm a maior adoção, puxada por propriedades rurais, que concentram 44% da potência no perfil "Agro solar".

## 4. Decisões recomendadas

| # | Decisão | Onde | Evidência |
|---|---|---|---|
| 1 | **Crédito solar com juros subsidiados e prazo compatível com a economia mensal** (a parcela cabe na conta de luz que deixa de ser paga) | 1.676 municípios do perfil "Muito sol, baixa renda", começando pelos de maior lacuna | Renda é o principal fator. O retorno por kWp é o mais alto do país |
| 2 | **Geração compartilhada e cooperativas solares** para quem não tem telhado próprio | Capitais com alta verticalização e lacuna: Salvador, Recife, Belo Horizonte, São Paulo | Lacuna concentrada em cidades com 28% a 39% de apartamentos |
| 3 | **Painéis solares em habitação social** (Minha Casa Minha Vida) e em prédios públicos com crédito revertido a famílias de baixa renda | Nordeste e perfil "Urbanos de baixa adoção" | Baixa renda é 1,3% dos sistemas residenciais registrados |
| 4 | **Exigir que as distribuidoras registrem a classe baixa renda** no cadastro de geração distribuída | ANEEL, todas as 102 distribuidoras (só 27 registram hoje) | Sem esse dado, não é possível medir se a política chega a quem precisa |
| 5 | **Usar o ranking de lacuna da camada Gold** como critério de priorização de municípios em editais | Tabela `gold/lacuna_adocao` | Compara cada município com outros de perfil semelhante |

## 5. Resultados a acompanhar

| Indicador | Linha de base | Meta sugerida (3 anos) | Fonte |
|---|---|---|---|
| Telhados solares por mil domicílios no Nordeste | 30 | 45 (metade da distância para o Centro-Oeste) | ANEEL + IBGE |
| Participação do Nordeste na potência nova dos últimos 12 meses | 26% | igual à participação na população (27%) | ANEEL |
| W por habitante no perfil "Muito sol, baixa renda" | 141 | 250 | ANEEL + IBGE |
| Telhados residenciais abaixo do esperado | 373 mil | redução de 30% | modelo da camada Gold |
| Distribuidoras que registram baixa renda | 27 de 102 | 102 de 102 | ANEEL |
| Sistemas de baixa renda no total residencial | 1,3% | 5% | ANEEL |

O pipeline é reexecutável todo mês com a nova base da ANEEL. Os indicadores acima são atualizados automaticamente e podem alimentar um painel de acompanhamento da política.

## Limitações

O PIB per capita é uma aproximação de renda das famílias. A data de conexão da ANEEL é a data da última atualização cadastral. Os últimos meses da base têm atraso de registro (tratado com corte automático). A classificação de baixa renda é subnotificada. O modelo de lacuna serve para priorizar, não como meta exata por município.
