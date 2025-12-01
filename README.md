# 📌 Navegador de Contratos – GECOBI

Aplicação em Python (Tkinter + MySQL/PyMySQL) desenvolvida para consultar, filtrar e navegar entre contratos das carteiras Autos, DivZero e Cedidas diretamente no banco GECOBI, de forma rápida, simples e automatizada.

## 🎯 Motivação do Projeto

Este projeto foi criado para automatizar um processo interno que antes tomava muito tempo da equipe.

Antes da ferramenta existir:

era necessário rodar consultas manuais no banco;

depois separar contratos por operador;

montar listas individualizadas;

enviar para cada operador sempre que precisassem de atualização.

Era um trabalho repetitivo, lento, suscetível a erros e que ocupava várias horas por semana.

Com o Navegador de Contratos:

cada operador seleciona seu próprio usuário diretamente no sistema;

o sistema carrega somente seus contratos, já filtrados e organizados;

toda consulta é feita em tempo real, direto no banco;

não há mais espera, filas ou dependência de terceiros.

O resultado é um processo mais rápido, organizado, eficiente e que trouxe autonomia total aos operadores.

## 🚀 Funcionalidades
🔎 Consultas inteligentes

Seleção de carteiras

Escolha opcional de operador (nomeusu)

Consulta automática e assíncrona no GECOBI

Carrega e organiza registros por:

Última data

Dados de acordo

Quantidade de propostas

Último CPC

Perfil do contrato (informações adicionais e flags)

## 🎚️ Filtros avançados

Quebrado / Rejeitado

CPC

Não acionado

Cor da última data:

Verde (≤ 7 dias)

Amarelo (8 a 30 dias)

Vermelho (> 30 dias)

## 🧭 Navegação prática

Próximo / Anterior

Ir para número específico

Duplo clique na linha para abrir o detalhe

Painel com informações completas do contrato

Perfil do cliente integrado

## 📧 E-mails e informações adicionais

Busca automática de e-mails do cod_cad

Correção de e-mails digitados incorretamente

Janela dedicada com botão de copiar selecionados/todos

## 💾 Exportação

Exportar toda a lista → CSV

Exportar somente a seleção → CSV

Copiar o registro atual no formato CSV

## 🎨 Interface personalizada

Tema claro/escuro

Seleção de tema nativo (clam, vista, xpnative etc.)

Preferências salvas automaticamente:

carteira(s)

operador escolhido

tema

modo escuro

## 🔐 Segurança

Nenhuma senha fica no código.

As credenciais são carregadas automaticamente do arquivo seguro:

\\fs01\ITAPEVA ATIVAS\DADOS\SA_Credencials.txt

# ▶️ Como executar

Instale o Python 3.10+

Instale as dependências:

pip install pandas pymysql


Garanta que o arquivo SA_Credencials.txt esteja disponível no caminho da rede.

Execute o programa:

python NavegadorContratos.py
