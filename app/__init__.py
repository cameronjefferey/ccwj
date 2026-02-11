from flask import Flask
from flask_login import LoginManager
from config import Config

app = Flask(__name__)
app.config.from_object(Config)

# Flask-Login setup
login_manager = LoginManager()
login_manager.login_view = 'login'
login_manager.login_message = 'Please log in to access this page.'
login_manager.login_message_category = 'info'
login_manager.init_app(app)


@login_manager.user_loader
def load_user(user_id):
    from app.models import User
    return User.get_by_id(int(user_id))


# Initialize the database and seed users from env
from app.models import init_db, seed_users_from_env
init_db()
seed_users_from_env()

from app import routes
from app import auth
from app import upload
