from app import create_app
from flask import render_template, redirect, url_for

app = create_app()

# Rota principal adicionada aqui
@app.route('/')
def index():
    """
    Esta função define o que acontece quando alguém acessa a página inicial.
    Ela simplesmente renderiza o seu template principal 'index.html'.
    """
    # Se o seu template principal estiver em outro caminho, ajuste aqui.
    # Ex: 'visualizador/index.html'
    return render_template('index.html')

if __name__ == '__main__':
    # O host='0.0.0.0' permite que você acesse de outros dispositivos na mesma rede
    app.run(host='0.0.0.0', port=5000, debug=True)