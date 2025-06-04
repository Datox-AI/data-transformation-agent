import os
import requests
from dotenv import load_dotenv
from typing import Literal
from langchain.tools.base import StructuredTool
from langchain_core.tools import tool

from src.transformation_agent.classes import (
    LogicalExpression,
    FilterAction,
    ReplaceExpression,
    ReplaceAction,
)

# from classes import (
    # LogicalExpression,
    # FilterAction,
    # ReplaceExpression,
    # ReplaceAction,
# )



load_dotenv(override=True)

base_url = os.getenv("BASE_URL")
transformation_id = os.getenv("TRANSFORMATION_ID")


@tool
def get_transformation_data():
    """
    Retrieves the current transformation data from the API.

    Returns:
        dict: A dictionary containing:
            - data: Transformed data in column-oriented format
            - steps: List of transformation steps applied
            - error: Optional error message if the last step failed
    """
    try:
        response = requests.get(f"{base_url}/api/transform/{transformation_id}")

        if response.status_code == 200:
            response_data = response.json()
            # transformed_data = response_data["data"]
            return change_response_data(response_data=response_data)
        else:
            return "Failed to get transformation data for some reason"
    except Exception as e:
        raise e
        return f"Failed to get transformation data: {e}"


@tool
def remove_last_step(number_of_steps: int):
    """
    Removes the recent transformation steps from the workflow.
    Receives:
        - number_of_steps: The number of steps to remove
    Returns:
        dict: Updated transformation data after removing the last step, including:
            - data: Transformed data in column-oriented format
            - steps: Updated list of transformation steps
            - error: Optional error message if the removal caused issues
    """
    try:
        transformation_steps = get_transformation_data.invoke(input=None)["steps"]
        if len(transformation_steps) > number_of_steps:
            number_of_steps = len(transformation_steps)
        for _ in range(number_of_steps):
            transformation_steps.pop()
        put_request_data = {"steps": transformation_steps}
        response = requests.put(
            f"{base_url}/api/transform/{transformation_id}", json=put_request_data
        )
        if response.status_code == 201:
            response_data = response.json()
            return change_response_data(
                response_data=response_data, check_last_step_for_error=True
            )
        else:
            return f"Failed to remove the last step: {response.status_code}"
    except Exception as e:
        return f"Failed to remove the last step: {e}"


def change_response_data(response_data: dict, check_last_step_for_error: bool = False):
    """
    Transforms the API response data into a more usable format.

    Args:
        response_data (dict): Raw response data from the API
        check_last_step_for_error (bool): Whether to check the last step for errors

    Returns:
        dict: Processed data containing:
            - data: Column-oriented data format (limited to first 3 rows)
            - steps: List of transformation steps
            - error: Optional error message from the last step
    """
    transformation_data = response_data["data"]

    if len(transformation_data) != 0:
        altered_transformed_data = {}
        column_row = transformation_data[0]
        for column in column_row.values():
            altered_transformed_data[column] = []
        index = 0
        for row in transformation_data:
            for key, value in row.items():
                altered_transformed_data[column_row[key]].append(value)
            index += 1
            if index > 2:
                break
    else:
        altered_transformed_data = {}

    transformation_data_with_steps = {
        "data": altered_transformed_data,
        "steps": response_data["steps"],
    }
    if check_last_step_for_error:
        if len(transformation_data_with_steps["steps"]) > 0 and transformation_data_with_steps["steps"][-1]["error"]:
            transformation_data_with_steps["error"] = (
                f"Error in the last step: {transformation_data_with_steps['steps'][-1]['error']}"
            )
    return transformation_data_with_steps


def add_filter_action_tool(expression: LogicalExpression):
    """
    Adds a filter transformation step to the workflow.

    Args:
        expression (LogicalExpression): The filter condition to apply

    Returns:
        dict: Updated transformation data after adding the filter, including:
            - data: Filtered data in column-oriented format
            - steps: Updated list of transformation steps
            - error: Optional error message if the filter step failed
    """
    try:
        filter_obj = FilterAction(expression=expression)
        filter_data = filter_obj.model_dump(by_alias=True, exclude_none=False)
        if filter_data["expression"]["and"] is None:
            del filter_data["expression"]["and"]
        else:
            del filter_data["expression"]["or"]
        filter_data["action"] = "filter"
        print(filter_data, " filter_data")

        # Get existing transformation steps
        transformation_steps = get_transformation_data.invoke(input=None)["steps"]
        # Add new filter step
        transformation_steps.append(filter_data)
        put_request_data = {"steps": transformation_steps}

        transformation_request_url = f"{base_url}/api/transform/{transformation_id}"
        response = requests.put(transformation_request_url, json=put_request_data)
        if response.status_code == 201:
            response_data = response.json()
            return change_response_data(
                response_data=response_data, check_last_step_for_error=True
            )
        else:
            print(response.__dict__)
            return f"Failed to get transformation data for some reason: {response.status_code}"
    except Exception as e:
        return f"Failed to add data: {e}"


def add_replace_action_tool(expression: ReplaceExpression, replace_column: str):
    """
    Adds a replace  transformation step to the workflow.

    Args:
        expression (ReplaceExpression): The replace condition to apply

    Returns:
        dict: Updated transformation data after adding replace action, including:
            - data: Filtered data in column-oriented format
            - steps: Updated list of transformation steps
            - error: Optional error message if the filter step failed
    """
    try:
        print(expression, " expressionss")
        replace_obj = ReplaceAction(
            expression=expression, replace_column=replace_column
        )
        replace_data = replace_obj.model_dump(by_alias=True, exclude_none=False)
        replace_data["expression"]["else"] = replace_data["expression"]["else_"]
        del replace_data["expression"]["else_"]
        replace_data["expression"]["replace_column"] = replace_column
        del replace_data["replace_column"]
        print(replace_data, " replace_data before")

        for when_condition in replace_data["expression"]["when"]:
            if when_condition["and"] is None:
                del when_condition["and"]
            else:
                del when_condition["or"]
        replace_data["action"] = "replace"
        print(replace_data, " replace_data after")

        # Get existing transformation steps
        transformation_steps = get_transformation_data.invoke(input=None)["steps"]
        # Add new filter step
        transformation_steps.append(replace_data)
        put_request_data = {"steps": transformation_steps}

        transformation_request_url = f"{base_url}/api/transform/{transformation_id}"
        response = requests.put(transformation_request_url, json=put_request_data)
        if response.status_code == 201:
            response_data = response.json()
            return change_response_data(
                response_data=response_data, check_last_step_for_error=True
            )
        else:
            print(response.__dict__)
            return f"Failed to get transformation data for some reason: {response.status_code}"
    except Exception as e:
        raise e
        return f"Failed to add data: {e}"


filter_tool = StructuredTool.from_function(
    func=add_filter_action_tool,
    name="Filter",
    description="Add filter to data",
    args_schema=FilterAction,
    return_direct=True,
)


replace_tool = StructuredTool.from_function(
    func=add_replace_action_tool,
    name="Replace",
    description="Add replace filter to data",
    args_schema=ReplaceAction,
    return_direct=True,
)


# data = {
#     "else_": {"column_name": "AUM"},
#     "when": [
#         {
#             "and": [
#                 {
#                     "column_name": "Security Category Industry Description",
#                     "operator": "equal to",
#                     "comparison_value": {"input": "FINANCIAL"},
#                 },
#                 {
#                     "column_name": "Issue currency",
#                     "operator": "does not contain",
#                     "comparison_value": {"input": "USD"},
#                 },
#             ],
#             "result_value": {"input": 0},
#         }
#     ],
# }



# res = replace_tool.invoke(input={"expression": data, "replace_column": "AUM"})

# print(res)