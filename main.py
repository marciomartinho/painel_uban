# main.py
from app import create_app
import os

# A aplicação agora é criada pela fábrica em app/__init__.py
app = create_app()

if __name__ == '__main__':
    # Define a porta para compatibilidade com a Railway e outros serviços
    port = int(os.environ.get('PORT', 5000))
    # Roda em modo de desenvolvimento local
    app.run(debug=True, host='0.0.0.0', port=port)