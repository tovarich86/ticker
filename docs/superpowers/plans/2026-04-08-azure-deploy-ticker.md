# Azure Deploy mf2-ticker Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Hospedar o Portal Financeiro B3/Tesouro (Streamlit) no Azure App Service `mf2-ticker` com CI/CD via Azure DevOps Pipelines, seguindo o mesmo padrão do projeto pria (`mf2-ia`).

**Architecture:** App Service Linux Python 3.11 com deploy via zip. O pipeline Azure DevOps faz build (pip install + git archive) e deploy (AzureWebApp@1 com startup.sh). O Streamlit escuta na porta 8000, que é a porta padrão do App Service Linux.

**Tech Stack:** Python 3.11, Streamlit, Azure App Service (Linux), Azure DevOps Pipelines, Azure CLI (opcional)

---

## Mapa de Arquivos

| Arquivo | Ação | Responsabilidade |
|---------|------|-----------------|
| `startup.sh` | Criar | Comando de startup para o App Service iniciar o Streamlit |
| `azure-pipelines.yml` | Criar | Pipeline CI/CD: build + deploy para mf2-ticker |

---

### Task 1: Criar `startup.sh`

**Files:**
- Create: `startup.sh`

- [ ] **Step 1: Criar o arquivo `startup.sh` na raiz do projeto**

Conteúdo exato:

```bash
#!/bin/bash
python -m streamlit run acoes.py \
  --server.port 8000 \
  --server.address 0.0.0.0 \
  --server.enableCORS false \
  --server.enableXsrfProtection false
```

- [ ] **Step 2: Verificar o conteúdo do arquivo**

```bash
cat startup.sh
```

Esperado: exibir o conteúdo acima sem erros.

- [ ] **Step 3: Commit**

```bash
git add startup.sh
git commit -m "feat: add startup.sh for Azure App Service"
```

---

### Task 2: Criar `azure-pipelines.yml`

**Files:**
- Create: `azure-pipelines.yml`

- [ ] **Step 1: Criar o arquivo `azure-pipelines.yml` na raiz do projeto**

Conteúdo exato:

```yaml
# Portal Financeiro B3/Tesouro — Azure App Service Deploy
# Padrão idêntico ao projeto pria (mf2-ia)

trigger:
  branches:
    include:
      - main

variables:
  azureServiceConnectionId: '0903325f-a8d0-49c6-8d09-9d0217b6dc0e'
  webAppName: 'mf2-ticker'
  vmImageName: 'ubuntu-latest'
  environmentName: 'mf2-ticker'
  projectRoot: $(System.DefaultWorkingDirectory)
  pythonVersion: '3.11'

stages:
- stage: Build
  displayName: Build stage
  jobs:
  - job: BuildJob
    pool:
      vmImage: $(vmImageName)
    steps:
    - task: UsePythonVersion@0
      inputs:
        versionSpec: '$(pythonVersion)'
      displayName: 'Use Python $(pythonVersion)'

    - script: |
        python -m pip install --upgrade pip
        pip install -r requirements.txt
      workingDirectory: $(projectRoot)
      displayName: 'Install requirements'

    - script: |
        git archive --format=zip --output=$(Build.ArtifactStagingDirectory)/$(Build.BuildId).zip HEAD
      workingDirectory: $(projectRoot)
      displayName: 'Empacotar aplicacao (git archive)'

    - upload: $(Build.ArtifactStagingDirectory)/$(Build.BuildId).zip
      displayName: 'Upload package'
      artifact: drop

- stage: Deploy
  displayName: 'Deploy Web App'
  dependsOn: Build
  condition: succeeded()
  jobs:
  - deployment: DeploymentJob
    pool:
      vmImage: $(vmImageName)
    environment: $(environmentName)
    strategy:
      runOnce:
        deploy:
          steps:
          - task: UsePythonVersion@0
            inputs:
              versionSpec: '$(pythonVersion)'
            displayName: 'Use Python version'

          - task: AzureWebApp@1
            displayName: 'Deploy Azure Web App: mf2-ticker'
            inputs:
              azureSubscription: $(azureServiceConnectionId)
              appName: $(webAppName)
              package: $(Pipeline.Workspace)/drop/$(Build.BuildId).zip
              startUpCommand: 'bash startup.sh'
```

- [ ] **Step 2: Verificar o conteúdo do arquivo**

```bash
cat azure-pipelines.yml
```

Esperado: exibir o YAML acima sem erros de sintaxe.

- [ ] **Step 3: Commit e push**

```bash
git add azure-pipelines.yml
git commit -m "feat: add Azure DevOps pipeline for mf2-ticker"
git push origin main
```

---

### Task 3: Importar repositório para o Azure DevOps

> Passo manual no browser — sem código.

- [ ] **Step 1: Acessar Azure DevOps**

Abrir: `https://pris.visualstudio.com/MF2/_git`

- [ ] **Step 2: Importar o repositório do GitHub**

1. Clicar em **Import repository** (ou no dropdown de repos → Import)
2. Source type: **Git**
3. Clone URL: `https://github.com/tovarich86/ticker.git`
4. Name: `ticker`
5. Clicar em **Import**

- [ ] **Step 3: Verificar o import**

Confirmar que o repo `ticker` aparece em `pris.visualstudio.com/MF2/_git/ticker` com todos os arquivos, incluindo `startup.sh` e `azure-pipelines.yml`.

> **Nota:** Se o repo do GitHub for privado, será necessário fornecer credenciais (Personal Access Token do GitHub) durante o import.

---

### Task 4: Criar o Azure App Service `mf2-ticker`

> Passo manual — via Azure Portal ou Azure CLI.

#### Opção A: Azure Portal (sem instalação)

- [ ] **Step 1: Acessar o Azure Portal**

Abrir: `https://portal.azure.com`

- [ ] **Step 2: Criar o App Service**

1. Buscar por **App Services** → **+ Create**
2. Preencher:
   - **Subscription:** mesma do pria
   - **Resource Group:** mesmo do pria (para compartilhar custos) ou criar novo `mf2-ticker-rg`
   - **Name:** `mf2-ticker`
   - **Publish:** Code
   - **Runtime stack:** Python 3.11
   - **Operating System:** Linux
   - **Region:** mesma do pria
   - **App Service Plan:** mesmo do pria (B1 ou superior) ou criar novo
3. Clicar em **Review + create** → **Create**

- [ ] **Step 3: Verificar criação**

Aguardar o deploy do resource e confirmar que a URL `https://mf2-ticker.azurewebsites.net` responde (mesmo que com página de boas-vindas padrão do Azure).

#### Opção B: Azure CLI (requer instalação prévia em https://aka.ms/installazurecliwindows)

- [ ] **Step alternativo: Criar via CLI**

```bash
az login
az group create --name mf2-ticker-rg --location brazilsouth
az appservice plan create --name mf2-ticker-plan \
  --resource-group mf2-ticker-rg --sku B1 --is-linux
az webapp create --name mf2-ticker \
  --resource-group mf2-ticker-rg \
  --plan mf2-ticker-plan \
  --runtime "PYTHON:3.11"
```

Esperado (último comando):
```json
{
  "name": "mf2-ticker",
  "state": "Running",
  ...
}
```

---

### Task 5: Criar Environment no Azure DevOps

> Passo manual no browser.

- [ ] **Step 1: Acessar Environments**

Abrir: `https://pris.visualstudio.com/MF2/_environments`

- [ ] **Step 2: Criar novo environment**

1. Clicar em **New environment**
2. Name: `mf2-ticker`
3. Resource: **None**
4. Clicar em **Create**

---

### Task 6: Criar e executar o Pipeline no Azure DevOps

> Passo manual no browser.

- [ ] **Step 1: Acessar Pipelines**

Abrir: `https://pris.visualstudio.com/MF2/_build`

- [ ] **Step 2: Criar novo pipeline**

1. Clicar em **New pipeline**
2. Selecionar **Azure Repos Git**
3. Selecionar o repositório `ticker`
4. Selecionar **Existing Azure Pipelines YAML file**
5. Branch: `main` | Path: `/azure-pipelines.yml`
6. Clicar em **Continue** → **Run**

- [ ] **Step 3: Aprovar o environment na primeira execução**

Na primeira execução, o stage Deploy pedirá aprovação para usar o environment `mf2-ticker`. Clicar em **Permit** quando solicitado.

- [ ] **Step 4: Verificar o deploy**

1. Aguardar os stages Build e Deploy completarem com sucesso (✅)
2. Acessar `https://mf2-ticker.azurewebsites.net`
3. Confirmar que a home do Portal Financeiro B3/Tesouro carrega corretamente com o menu lateral do Streamlit

---

## Verificação Final

Após todos os tasks concluídos:

- [ ] `https://mf2-ticker.azurewebsites.net` abre a home page do Portal Financeiro
- [ ] Menu lateral mostra todas as 7 páginas (Busca Ativos, Taxas DI, Inflação Implícita, Calculadora IPCA, Volatilidade, TSR, TBOND)
- [ ] Push em `main` no Azure DevOps dispara novo pipeline automaticamente
- [ ] Pipeline completa Build + Deploy sem erros

---

## Troubleshooting

| Problema | Causa provável | Solução |
|----------|---------------|---------|
| App Service retorna HTTP 503 | Streamlit não iniciou | Verificar logs em App Service → Log stream; checar se `startup.sh` está no zip |
| Pipeline falha em "Install requirements" | Dependência incompatível com Linux | Verificar `requirements.txt`; `curl_cffi` pode precisar de versão específica |
| Pipeline falha em "Deploy" | Service connection sem permissão | Verificar que `0903325f-a8d0-49c6-8d09-9d0217b6dc0e` tem acesso ao App Service `mf2-ticker` |
| `startup.sh` não encontrado | git archive não incluiu o arquivo | Confirmar que `startup.sh` está commitado no repo (`git ls-files startup.sh`) |
| Porta errada | App escutando em 8501 em vez de 8000 | Confirmar que `startup.sh` usa `--server.port 8000` |
