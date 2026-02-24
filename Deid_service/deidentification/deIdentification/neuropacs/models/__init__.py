from .patient import Patients
from .study import PatientStudy
from .series import PatientSeries
from .instance import PatientInstance
from .pacsclient import PacsClient


# I need this type of view for page
# /Users/rohitchouhan/Documents/Code/backend/de-identification-ui/app/clients/[clientId]/pacs/[pacsClientId]/studies/page.tsx

# patients call
# localhost:8000/pacs/patients/<pacs_client_id>/
# response:
# {
#     "count": 10,
#     "num_pages": 1,
#     "current_page": 1,
#     "results": [
#         {
#             "id": 3,
#             "nd_patient_id": null,
#             "client_patient_id": 9,
#             "deid_status": 0,
#             "cloud_uploaded": false,
#             "study_uids": [
#                 "1.2.826.0.1.3680043.8.498.13198449042664663506829823769415302512",
#                 "1.2.826.0.1.3680043.8.498.86553976305553480765269083316898967504",
#                 "1.2.826.0.1.3680043.8.498.28241685572025843658636645110242563443"
#             ]
#         }
#     ]
# }


# studies call
# localhost:8000/pacs/studies/<pacs_client_id/<client_patient-id>/
# {
#     "count": 3,
#     "num_pages": 1,
#     "current_page": 1,
#     "results": [
#         {
#             "id": 22,
#             "client_study_instance_uid": "1.2.826.0.1.3680043.8.498.55104324176898169223444431820398844669",
#             "nd_study_instance_uid": null,
#             "deid_status": 0,
#             "cloud_uploaded": false,
#             "series_uids": [
#                 "1.2.826.0.1.3680043.8.498.77052603981961996959390811273851440349"
#             ]
#         }
#     ]
# }

# series call
# localhost:8000/pacs/series/<pacs_client_id>/<client_patient_id>/<client_study_uid>/
# {
#     "count": 1,
#     "num_pages": 1,
#     "current_page": 1,
#     "results": [
#         {
#             "id": 24,
#             "client_series_instance_uid": "1.2.826.0.1.3680043.8.498.38055210091528511313285600533871459450",
#             "nd_series_instance_uid": null,
#             "deid_status": 0,
#             "cloud_uploaded": false,
#             "instance_uids": [
#                 "1.2.826.0.1.3680043.8.498.55008494500778294456257970507630941314",
#                 "1.2.826.0.1.3680043.8.498.94426527773424162660183980853229985283"
#             ]
#         }
#     ]
# }

# instance call
# localhost:8000/pacs/instance/<pacs_clientid>/<clientPatient_id>/<client_study_uid>/<client_series_uid>/
# {
#     "count": 2,
#     "num_pages": 1,
#     "current_page": 1,
#     "results": [
#         {
#             "id": 47,
#             "client_sop_instance_uid": "1.2.826.0.1.3680043.8.498.55008494500778294456257970507630941314",
#             "nd_sop_instance_uid": null,
#             "deid_status": 0,
#             "cloud_uploaded": false
#         }
#     ]
# }
