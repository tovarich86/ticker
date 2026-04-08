# Design: Deploy ticker no Azure como Web App

**Data:** 2026-04-08
**Autor:** Danilo da Cruz Ferraz (PRIS SOFTWARE LTDA)

---

## Objetivo

Hospedar o Portal Financeiro B3/Tesouro (app Streamlit) no Azure App Service, seguindo exatamente o mesmo padrão do projeto "pria" (`mf2-ia`), com CI/CD via Azure DevOps Pipelines.

---

## Arquitetura

```
GitHub (tovarich86/ticker)
        │
        │  (import manual)
        ▼
Azure DevOps — pris.visualstudio.com/MF2/_git/ticker
        │
        │  trigger: push em main
        ▼
Azure DevOps Pipeline (azure-pipelines.yml)
  ├── Stage Build:
  │     • UsePythonVersion 3.11
  │     • pip install -r requirements.txt
  │     • git archive → .zip
  │     • upload artifact: drop
  └── Stage Deploy:
        • AzureWebApp@1
        • startUpCommand: bash startup.sh
        │
        ▼
Azure App Service: mf2-ticker
  • OS: Linux
  • Runtime: Python 3.11
  • URL: https://mf2-ticker.azurewebsites.net
  • Startup: streamlit run acoes.py --server.port 8000
```

---

## Arquivos a criar no repositório

### `startup.sh`

Script executado pelo App Service ao iniciar a aplicação. Inicia o Streamlit na porta 8000 (porta padrão do Azure App Service Linux).

```bash
#!/bin/bash
python -m streamlit run acoes.py \
  --server.port 8000 \
  --server.address 0.0.0.0 \
  --server.enableCORS false \
  --server.enableXsrfProtection false
```

### `azure-pipelines.yml`

Pipeline CI/CD idêntico ao pria, sem step de testes (o projeto ticker não possui pasta `tests/`).

```yaml
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

---

## Passos de execução (em ordem)

### 1. Criar repo no Azure DevOps
- Acessar `pris.visualstudio.com/MF2` → Repos → Import Repository
- Source: `https://github.com/tovarich86/ticker.git`
- Nome: `ticker`

### 2. Criar o Azure App Service `mf2-ticker`
**Via Azure Portal:**
- App Services → + Create
- Resource Group: mesmo do pria ou novo `mf2-ticker-rg`
- Name: `mf2-ticker`
- Runtime: Python 3.11 | Linux
- Region: mesma do pria
- App Service Plan: mesmo do pria (compartilhado) ou novo B1

**Via Azure CLI** (requer instalação: https://aka.ms/installazurecliwindows):
```bash
az group create --name mf2-ticker-rg --location brazilsouth
az appservice plan create --name mf2-ticker-plan \
  --resource-group mf2-ticker-rg --sku B1 --is-linux
az webapp create --name mf2-ticker \
  --resource-group mf2-ticker-rg \
  --plan mf2-ticker-plan --runtime "PYTHON:3.11"
```

### 3. Criar environment no Azure DevOps
- Azure DevOps → Pipelines → Environments → New environment
- Name: `mf2-ticker`

### 4. Adicionar os arquivos ao repo e fazer push
- Criar `startup.sh` e `azure-pipelines.yml` na raiz do projeto
- Commit e push para `main`

### 5. Criar o pipeline no Azure DevOps
- Pipelines → New Pipeline → Azure Repos Git → `ticker`
- Selecionar "Existing Azure Pipelines YAML file" → `/azure-pipelines.yml`
- Run pipeline

---

## Configurações do App Service

| Parâmetro | Valor |
|-----------|-------|
| Nome | `mf2-ticker` |
| OS | Linux |
| Runtime | Python 3.11 |
| Porta | 8000 (padrão App Service Linux) |
| Startup command | `bash startup.sh` |
| URL | `https://mf2-ticker.azurewebsites.net` |

---

## Referências

- Padrão baseado em: `pris.visualstudio.com/MF2/_git/IA` (projeto pria / `mf2-ia`)
- Service connection reutilizado: `0903325f-a8d0-49c6-8d09-9d0217b6dc0e`
- App principal: `acoes.py` (Home page Streamlit)
