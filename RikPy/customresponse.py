class CustomResponse:
    def __init__(self, data, status_code):
        self.data = data
        self.status_code = status_code
        self.text = str(data)

    def json(self):
        try:
            return self.data
        except ValueError:
            raise Exception("Invalid JSON")