from rest_framework.test import APITestCase
from django.urls import reverse
from nd_api.models import Clients


class RegisterNewDumpViewTests(APITestCase):

    def test_register_new_db(self):
        client_config = {"connection_str": ""}
        mapping_db_config = {"connection_str": ""}
        master_db_config = {"connection_str": ""}
        client_obj, created = Clients.objects.get_or_create(
            client_name="test_local", emr_type="athenone",
            config=client_config, mapping_db_config=mapping_db_config, master_db_config=master_db_config
        )
        url = reverse("register_new_db", kwargs={"client_id": client_obj.id})

        # Example payload (adjust based on your view's expectations)
        payload = {
            "dump_name": "test_dumps",
            "source_db_config": {"connection_str": "mysql://root:123456789@localhost/nddenttest"},
            "admin_db_config": {"connection_str": "mysql://root:123456789@localhost/nddenttest"},
            "run_config": {},
            "pii_config": {},
            "qc_config": {}
        }

        response = self.client.post(url, payload, format='json')
        self.assertEqual(response.status_code, 200)