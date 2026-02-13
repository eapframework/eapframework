import requests
import pandas as pd
from transforms.api import transform, Output, TransformContext, TransformOutput

TOKEN = "eyJhbGciOiJFUzI1NiJ9.eyJzdWIiOiIveXFzeEdsN1FodWVRc291cFpkcW1RPT0iLCJqdGkiOiJLTGE3YXpmY1NUS0dkdzlReFQ5RElRPT0ifQ.AkoEH8rVvWnubdvAIMCqMIC-MZpIIHKhIlyKrrRrC0grjvYClIoG8ZjFNMDVSccjwnMWNdrnsKXxA1ym2Cel5Q"
LINEAGE_URL = "https://paloma.palantirfoundry.com/monocle/api/links/graphV2"
COMPASS_URL = "https://paloma.palantirfoundry.com/compass/api/resources/batch/branches-by-name"
BRANCH_NAME = ["master"]

@transform(
    output=Output("/Innovation Lab/[Source] DEEP_INGESTION/logic/datasets/Next/Lineage"),
)
def compute(ctx: TransformContext, output: TransformOutput):
    start_rid = "ri.workshop.main.module.f59d3eb6-d64f-43cd-ba18-0298e28768b1"

    headers = {
        "accept": "application/json",
        "authorization": f"Bearer {TOKEN}",
        "content-type": "application/json"
    }

    def extract_incoming_and_outgoing_links(data):
        incoming = []
        outgoing_map = {}  # node_rid -> list of outgoing rids
        for node in data.get("nodes", []):
            node_rid = node.get("resourceIdentifier")
            outgoing_rids = []
            for link in node.get("links", []):
                # Only process links where inTrash is False
                if link.get("inTrash", False):
                    continue
                link_type = link.get("type")
                for v in link.values():
                    if isinstance(v, dict):
                        direction = v.get("linkDirection")
                        # Select the correct RID based on link_type
                        if link_type == "datasetLink":
                            rid = v.get("resourceIdentifier")
                        elif link_type == "ontologyLink":
                            rid = v.get("objectTypeId")
                        elif link_type == "objectProvenanceLink":
                            rid = v.get("resourceIdentifier")
                        else:
                            rid = v.get("resourceIdentifier")
                        if direction == "INCOMING" and rid:
                            incoming.append((rid, link_type, node_rid))
                        elif direction == "OUTGOING" and rid:
                            outgoing_rids.append(rid)
            if node_rid:
                outgoing_map[node_rid] = outgoing_rids
        return incoming, outgoing_map

    def get_incoming_and_outgoing_batch(rids):
        if not rids:
            return [], {}
        payload = {
            "resourceIdentifiers": rids,
            "branch": "master",
            "fallbacks": [],
            "serviceTypeFilter": []
        }
        resp = requests.post(LINEAGE_URL, headers=headers, json=payload)
        resp.raise_for_status()
        data = resp.json()
        return extract_incoming_and_outgoing_links(data)

    # 1. Traverse lineage
    all_incoming = dict()  # rid -> (type, step_number, parent_node_rid)
    outgoing_map_total = {}  # node_rid -> list of outgoing rids
    seen = set([start_rid])
    to_process = [(start_rid, 0)]  # (rid, step_number)

    while to_process:
        current_rids = [rid for rid, _ in to_process]
        current_step = to_process[0][1] + 1 if to_process else 1
        incoming_rids_and_types, outgoing_map = get_incoming_and_outgoing_batch(current_rids)
        outgoing_map_total.update(outgoing_map)
        next_to_process = []
        for rid, link_type, node_rid in incoming_rids_and_types:
            if rid not in seen:
                all_incoming[rid] = (link_type, current_step, node_rid)
                next_to_process.append((rid, current_step))
                seen.add(rid)
        to_process = next_to_process

    # 2. For all unique incoming_rid, get resource_type, resource_id, file, node_id
    def get_resource_metadata(rid_list):
        # Batch up to 100 at a time for efficiency
        results = {}
        for i in range(0, len(rid_list), 100):
            batch = rid_list[i:i+100]
            payload = {
                "rids": batch,
                "names": BRANCH_NAME
            }
            compass_headers = {
                "Authorization": f"Bearer {TOKEN}",
                "Content-Type": "application/json",
            }
            resp = requests.post(COMPASS_URL, json=payload, headers=compass_headers)
            resp.raise_for_status()
            data = resp.json()
            # Parse as in code_rid.py
            if "branchesByName" in data:
                items = data.get("branchesByName", [])
                for item in items:
                    input_rid = item.get("rid")
                    resource_id = None
                    resource_type = None
                    file = None
                    node_id = None
                    branches_by_name = item.get("branchesByName", {})
                    if not branches_by_name:
                        if input_rid and input_rid.startswith("ri.workshop.main.module"):
                            resource_id = input_rid
                            resource_type = "Workshop"
                        elif input_rid and input_rid.startswith("ri.fusion.main.document"):
                            resource_id = input_rid
                            resource_type = "FusionSheet"
                        results[input_rid] = (resource_type, resource_id, file, node_id)
                        continue
                    for branch in BRANCH_NAME:
                        branch_list = branches_by_name.get(branch, [])
                        for branch_obj in branch_list:
                            branch_data = branch_obj.get("branch", {})
                            url_vars = branch_data.get("urlVariables", {})
                            build = url_vars.get("build", {})
                            if "code" in build:
                                code_dict = build["code"]
                                repo_rid = code_dict.get("repositoryRid")
                                file_tag = code_dict.get("file")
                                if repo_rid:
                                    resource_id = repo_rid
                                    resource_type = "Transform"
                                    file = file_tag
                            elif "eddiePipeline" in build:
                                pipeline_dict = build["eddiePipeline"]
                                pipeline_rid = pipeline_dict.get("pipelineRid")
                                if pipeline_rid:
                                    resource_id = pipeline_rid
                                    resource_type = "PipelineBuilder"
                            elif "contour" in build:
                                contour_dict = build["contour"]
                                contour_id = contour_dict.get("analysisId")
                                node_tag = contour_dict.get("nodeId")
                                resource_id = contour_id
                                resource_type = "Contour"
                                node_id = node_tag
                            elif isinstance(url_vars, dict) and url_vars.get("schema") is True and "build" not in url_vars:
                                resource_id = input_rid
                                resource_type = "Uploaded dataset"
                            elif isinstance(build, dict) and len(build) == 0:
                                resource_id = input_rid
                                resource_type = "RawDataset"
                            # Only take the first match per rid
                            if resource_id:
                                results[input_rid] = (resource_type, resource_id, file, node_id)
                                break
                        if input_rid in results:
                            break
            # fallback for "branches"
            elif "branches" in data:
                for dataset_rid, branches in data["branches"].items():
                    resource_id = dataset_rid
                    resource_type = None
                    file = None
                    node_id = None
                    for branch in BRANCH_NAME:
                        branch_list = branches.get(branch, [])
                        for branch_obj in branch_list:
                            build = branch_obj.get("branch", {}).get("urlVariables", {}).get("build", {})
                            if isinstance(build, dict) and len(build) == 0:
                                resource_type = "RawDataset"
                                break
                        if resource_type:
                            break
                    results[dataset_rid] = (resource_type, resource_id, file, node_id)
        return results

    rid_list = list(all_incoming.keys())
    rid_metadata = get_resource_metadata(rid_list)

    # 3. Prepare DataFrame
    records = []
    for rid, (link_type, step, parent_node_rid) in all_incoming.items():
        resource_type, resource_id, file, node_id = rid_metadata.get(rid, (None, None, None, None))
        # If resource_type is empty, infer from incoming_rid
        if not resource_type or resource_type == "":
            if "source" in rid:
                resource_type = "DataSource"
            elif "agent" in rid:
                resource_type = "Agent"
            elif "ontology" in rid:
                resource_type = "OntologyObject"
        outgoing_rids = outgoing_map_total.get(rid, [])
        records.append({
            "incoming_rid": rid,
            "type": link_type,
            "step_number": step,
            "resource_type": resource_type,
            "resource_id": resource_id,
            "file": file,
            "node_id": node_id,
            "outgoing_rid": outgoing_rids
        })

    df = pd.DataFrame(records)

    # Filter outgoing_rid to only those present in incoming_rid, unless it contains 'ri.workshop'
    incoming_rid_set = set(df["incoming_rid"])
    df["outgoing_rid"] = df["outgoing_rid"].apply(
        lambda rids: [rid for rid in rids if (rid in incoming_rid_set) or ("ri.workshop" in rid)]
    )

    output.write_dataframe(ctx.spark_session.createDataFrame(df))
