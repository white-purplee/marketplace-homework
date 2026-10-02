from flask import Flask
app = Flask(__name__)
app.add_url_rule("/health", view_func=lambda: {"status": "ok", "service": "catalog"})
