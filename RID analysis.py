import requests
import pandas as pd
from transforms.api import transform, Output, TransformContext, TransformOutput
from pyspark.sql.types import StructType, StructField, StringType

RID_LIST = [
    #"ri.foundry.main.dataset.f11e833f-5e1f-40bb-8bd4-197c8ac976c7",
     "ri.workshop.main.module.f59d3eb6-d64f-43cd-ba18-0298e28768b1",
    # "ri.contour.main.analysis.7dd771ad-597d-4681-a030-c49fbf624e28",
]

branch_name = ["master"]
API_URL = "https://paloma.palantirfoundry.com/compass/api/resources/batch/branches-by-name"

@transform(
    output=Output("/Innovation Lab/[Source] DEEP_INGESTION/logic/datasets/Next/RID_Analysis"),
)
def compute(ctx: TransformContext, output: TransformOutput):
    token = "eyJhbGciOiJFUzI1NiJ9.eyJzdWIiOiIveXFzeEdsN1FodWVRc291cFpkcW1RPT0iLCJqdGkiOiJLTGE3YXpmY1NUS0dkdzlReFQ5RElRPT0ifQ.AkoEH8rVvWnubdvAIMCqMIC-MZpIIHKhIlyKrrRrC0grjvYClIoG8ZjFNMDVSccjwnMWNdrnsKXxA1ym2Cel5Q"

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    payload = {
        "rids": RID_LIST,
        "names": branch_name
    }

    response = requests.post(API_URL, json=payload, headers=headers)
    response.raise_for_status()
    data = response.json()

    # Prepare lists for output
    resource_ids = []
    resource_types = []
    files = []
    nodes = []
    input_rids = []

    def get_dataset_rid(item):
        rid = item.get("rid")
        if rid:
            return rid
        return None

    if "branchesByName" in data:
        items = data.get("branchesByName", [])
        for item in items:
            input_rid = item.get("rid")
            branches_by_name = item.get("branchesByName", {})
            if not branches_by_name:
                if input_rid and input_rid.startswith("ri.workshop.main.module"):
                    resource_ids.append(input_rid)
                    resource_types.append("Workshop")
                    files.append(None)
                    nodes.append(None)
                    input_rids.append(input_rid)
                elif input_rid and input_rid.startswith("ri.fusion.main.document"):
                    resource_ids.append(input_rid)
                    resource_types.append("FusionSheet")
                    files.append(None)
                    nodes.append(None)
                    input_rids.append(input_rid)
                continue
            for branch in branch_name:
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
                            resource_ids.append(str(repo_rid) if repo_rid is not None else None)
                            if file_tag and 'transforms-python' in file_tag:
                                resource_types.append("PythonTransformation")
                            else:
                                resource_types.append("Transformation")
                            files.append(str(file_tag) if file_tag is not None else None)
                            nodes.append(None)
                            input_rids.append(str(input_rid) if input_rid is not None else None)
                    elif "eddiePipeline" in build:
                        pipeline_dict = build["eddiePipeline"]
                        pipeline_rid = pipeline_dict.get("pipelineRid")
                        if pipeline_rid:
                            resource_ids.append(str(pipeline_rid) if pipeline_rid is not None else None)
                            resource_types.append("PipelineBuilder")
                            files.append(None)
                            nodes.append(None)
                            input_rids.append(str(input_rid) if input_rid is not None else None)
                    elif "contour" in build:
                        contour_dict = build["contour"]
                        contour_id = contour_dict.get("analysisId")
                        node_tag = contour_dict.get("nodeId")
                        resource_ids.append(str(contour_id) if contour_id is not None else None)
                        resource_types.append("Contour")
                        files.append(None)
                        nodes.append(str(node_tag) if node_tag is not None else None)
                        input_rids.append(str(input_rid) if input_rid is not None else None)
                    elif isinstance(url_vars, dict) and url_vars.get("schema") is True and "build" not in url_vars:
                        dataset_rid = get_dataset_rid(item)
                        if dataset_rid is None:
                            dataset_rid = branch_data.get("rid")
                        resource_ids.append(str(dataset_rid) if dataset_rid is not None else None)
                        resource_types.append("UploadedDataset")
                        files.append(None)
                        nodes.append(None)
                        input_rids.append(str(input_rid) if input_rid is not None else None)
                    elif isinstance(build, dict) and len(build) == 0:
                        dataset_rid = get_dataset_rid(item)
                        if dataset_rid is None:
                            dataset_rid = branch_data.get("rid")
                        resource_ids.append(str(dataset_rid) if dataset_rid is not None else None)
                        resource_types.append("RawDataset")
                        files.append(None)
                        nodes.append(None)
                        input_rids.append(str(input_rid) if input_rid is not None else None)
                    else:
                        continue
    elif "branches" in data:
        for dataset_rid, branches in data["branches"].items():
            for branch in branch_name:
                branch_list = branches.get(branch, [])
                for branch_obj in branch_list:
                    build = branch_obj.get("branch", {}).get("urlVariables", {}).get("build", {})
                    if isinstance(build, dict) and len(build) == 0:
                        resource_ids.append(str(dataset_rid) if dataset_rid is not None else None)
                        resource_types.append("RawDataset")
                        files.append(None)
                        nodes.append(None)
                        input_rids.append(str(dataset_rid) if dataset_rid is not None else None)
    else:
        pass

    # Define schema
    schema = StructType([
        StructField("input_rid", StringType(), True),
        StructField("resource_id", StringType(), True),
        StructField("resource_type", StringType(), True),
        StructField("file", StringType(), True),
        StructField("node_id", StringType(), True),
    ])

    if len(input_rids) == 0:
        empty_df = ctx.spark_session.createDataFrame([], schema)
        output.write_dataframe(empty_df)
    else:
        df = pd.DataFrame({
            "input_rid": input_rids,
            "resource_id": resource_ids,
            "resource_type": resource_types,
            "file": files,
            "node_id": nodes
        })
        # Cast all columns to string to avoid type inference issues
        for col in df.columns:
            df[col] = df[col].astype(str)
        spark_df = ctx.spark_session.createDataFrame(df, schema=schema)
        output.write_dataframe(spark_df)
