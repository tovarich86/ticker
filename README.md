 Ticker Data App
Este é um aplicativo web desenvolvido em Python usando Streamlit que permite buscar dados de ações e dividendos de múltiplos tickers simultaneamente. O aplicativo é ideal para quem trabalha com Investimento de Longo Prazo (ILP) e precisa mensurar TSR (Total Shareholder Return) de forma rápida e eficiente.

 Funcionalidades
Busca de Dados de Ações: Permite buscar dados históricos de ações de múltiplos tickers de uma só vez, usando a biblioteca yfinance.
Busca de Dividendos: Integração com a API da B3 para buscar dados de dividendos de ações listadas.
Download de Resultados: Possibilidade de baixar os dados de ações e dividendos em um arquivo Excel consolidado.
Interface Amigável: Construído com Streamlit, oferece uma interface intuitiva e fácil de usar.
🛠 Instalação
Clone o repositório:

bash
Copiar código
git clone https://github.com/seu-usuario/ticker-data-app.git
cd ticker-data-app
Instale as dependências:

bash
Copiar código
pip install -r requirements.txt
Execute o aplicativo:

bash
Copiar código
streamlit run acoes.py
📄 Uso
Abra o aplicativo em seu navegador.
Digite os tickers das ações que deseja buscar, separados por vírgula (ex: PETR4, VALE3, ABEV3).
Informe a data de início e de fim para a busca de dados.
Opte por buscar ou não os dividendos no período selecionado.
Clique em "Buscar Dados" para visualizar os resultados e fazer o download do Excel.
📚 Requisitos
Python 3.7 ou superior
Pacotes Python listados em requirements.txt
Conexão com a internet para buscar dados
📝 Contribuição
Contribuições são bem-vindas! Sinta-se à vontade para abrir issues, enviar pull requests ou sugerir novas funcionalidades.

Faça um fork do projeto.
Crie sua feature branch: git checkout -b minha-nova-feature
Faça commit das suas alterações: git commit -m 'Adicionei uma nova feature'
Envie para o branch principal: git push origin minha-nova-feature
Abra um Pull Request.
⚖️ Licença
