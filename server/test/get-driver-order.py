from locust import HttpUser, task


class LoginUser(HttpUser):
    @task
    def login(self):
        url = "/driver_order/get_driver_orders"
        self.client.get(
            url,
            headers={
                "X-Deliver-Auth": "GpDXF_jAu3jkfa-8.rpllcPzuoKKEhJ1vDTdy5T_UkXQ0AhE2p-m0Lxe2NRFXyl0O0jw-74w3p_upZ8V-38YBsdBdB5iPggeFjBGi-dV6U2T5BhxfaXoSadxXlvn-jYRgO9a79xyBOyHgSQNgZ_gwOnd1Mh773oInbQkOcyrsYZ4ICblnSj5yrzyy_6h0U9QJ9psWqR9P1-oDb3tMUck"
            },
        )
