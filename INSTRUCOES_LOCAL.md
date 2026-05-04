# Guia de Execução Local - Auditoria Bolsa Família

Este documento explica como rodar o sistema utilizando o seu computador local como servidor de banco de dados, contornando bloqueios de firewall da Vercel.

## 🚀 Como Iniciar o Sistema

Sempre que quiser usar o sistema, abra o terminal (Prompt de Comando) e execute os seguintes passos:

### 1. Entrar na pasta do projeto
```cmd
cd Desktop\bolsafamilia\consultasBolsaFamilia
```

### 2. Iniciar o Servidor (API)
```cmd
python api/index.py
```
*Mantenha esta janela aberta enquanto estiver usando o site.*

---

## 🌐 Como Configurar o Navegador

Como o site oficial (`https`) vai tentar conversar com o seu computador local (`http`), o Chrome vai bloquear por segurança. Siga estes passos **uma única vez**:

1. Abra o site: [https://consultas-bolsa-familia.vercel.app/](https://consultas-bolsa-familia.vercel.app/)
2. Clique no ícone de **Cadeado** (ou configurações) à esquerda da URL na barra de endereços.
3. Clique em **Configurações do site**.
4. Procure por **"Conteúdo inseguro"** e mude para **"Permitir"**.
5. Volte ao site e dê um **F5 (Recarregar)**.

---

## ⚙️ Configuração no Site

Dentro do site da Vercel:
1. Clique em **Configurações avançadas** (ícone de engrenagem no painel esquerdo).
2. No campo **"URL da API (para uso local)"**, digite: `http://localhost:8000`
3. Selecione o município e clique em **Iniciar Cruzamento**.

---

## 🛠️ Resumo das Correções Realizadas

1. **Estabilização do Oracle**: Implementamos um sistema de 5 tentativas com espera automática para evitar o erro `EBUSY` (Recurso Ocupado).
2. **Correção de Colunas**: Removemos o campo `PESS_DATA_ADMISSAO` que não existia no seu banco, baseando-nos na query de exemplo que você forneceu.
3. **Migração de Python**: Atualizamos a instalação do Python para a versão **3.12 (Estável)**, pois a versão 3.14 experimental estava corrompida.
4. **Ponte Local-Nuvem**: Criamos uma funcionalidade no Frontend que permite usar o site da Vercel apontando para o banco de dados que roda no seu PC, resolvendo o problema de acesso ao Oracle de fora da rede.

**Dúvidas ou Erros?** Basta me chamar!
