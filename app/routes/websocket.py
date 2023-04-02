from app.routes.endpoints.websocket import routes as routes_websocket


routes = [
    *routes_websocket(),
]
