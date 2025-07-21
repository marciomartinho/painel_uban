from flask import Blueprint, render_template

main_bp = Blueprint('main', __name__)

@main_bp.route('/')
def index():
    """
    Esta função define a rota principal ('/') e renderiza a sua
    página de menu 'index.html'.
    """
    return render_template('index.html')