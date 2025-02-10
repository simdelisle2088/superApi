from locust import HttpUser, task


class LoginUser(HttpUser):
    @task
    def login(self):
        url = "/register/login"
        data = {"username": "test", "password": "password", "vehicule": 1}
        self.client.post(url, json=data)
