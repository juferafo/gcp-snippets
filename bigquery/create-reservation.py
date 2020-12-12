"""
This script creates a slot reservation in BigQuery

Te BE DONE BY THE DEVELOPER:

1 - Modify the PROJECT_ID
2 - Modify the REGION
"""

from google.cloud.bigquery.reservation_v1 import *
from google.api_core import retry
import time


slots = 500
reservation_name = "sample-reservation"
user_project = "PROJECT_ID"
region = "REGION"

res_api = ReservationServiceClient()
parent_arg = "projects/{}/locations/{}".format(project, region)
commit_config = CapacityCommitment(plan="FLEX", slot_count=slots)
commit = res_api.create_capacity_commitment(parent=parent_arg,
                                          capacity_commitment=commit_config)

res_config = Reservation(slot_capacity=slots, ignore_idle_slots=False)
res = res_api.create_reservation(parent=parent_arg, 
                               reservation_id=reservation_name,
                               reservation=res_config)  

assign_config = Assignment(job_type="QUERY",
                         assignee="projects/{}".format(user_project))
assign = res_api.create_assignment(parent=res.name,
                                 assignment=assign_config)
