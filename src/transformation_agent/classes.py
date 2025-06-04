from pprint import pprint 
from typing import List, Union, Optional, Literal
from pydantic import BaseModel, Field, model_validator


class ComparisonValue(BaseModel):
    """
    Represents the value used for comparison in a condition.
    It can be a direct input, a datetime value, or a reference to another column.
    """
    
    input: Optional[Union[str, int, float, bool]] = Field(
        None, description="A direct input value (string, number, boolean)."
    )
    datetime: Optional[str] = Field(
        None, description="Datetime value for temporal comparisons."
    )
    column_name: Optional[str] = Field(
        None, description="Another column name to compare against."
    )


class Condition(BaseModel):
    """A basic conditional expression applied to a single column in the dataset."""

    column_name: str = Field(..., description="The column to apply the condition to.")
    operator: Literal[
        "equal to", "not equal to", "greater than", "less than",
        "greater than or equal to", "less than or equal to",
        "contains", "does not contain", "starts with", "does not start with",
        "ends with", "does not end with"
    ] = Field(..., description="The operator used for the comparison.")
    comparison_value: ComparisonValue = Field(..., description="The value used for comparison.")



class LogicalExpression(BaseModel):
    """
    Represents a logical grouping of conditions or nested logical expressions.
    - Required: Either `and` or `or`, but not both.
    - If there's only one expression, use `and` by default.
    """

    and_: Optional[List[Union['LogicalExpression', Condition]]] = Field(
        None, alias="and", description="Conditions or expressions combined with AND."
    )
    or_: Optional[List[Union['LogicalExpression', Condition]]] = Field(
        None, alias="or", description="Conditions or expressions combined with OR."
    )


    class Config:
        populate_by_name = True


    def validate_structure(self):
        if not self.and_ and not self.or_:
            raise ValueError("Either 'and' or 'or' must be present in LogicalExpression.")

    def __init__(self, **data):
        super().__init__(**data)
        self.validate_structure()


class FilterAction(BaseModel):
    """Represents a filter action that applies a logical expression to filter dataset rows."""
    # action: Literal["filter"] = Field(..., description="Action type.")
    expression: LogicalExpression = Field(..., description="Logical expression combining conditions.")



class ReplaceResultValue(ComparisonValue):
    """
    Represents the value to be used in a replacement operation 
    when a condition in a logical expression is met.
    """
    pass


class ReplaceCondition(Condition):
    function: Union[None, Literal["length"]] = Field(
        None,
        description=(
            "Optional transformation function applied to the column before comparison. "
            "Currently only supports 'length' to evaluate the string length of the column value."
        )
    )


class ReplaceLogicalExpression(LogicalExpression):
    result_value: ReplaceResultValue = Field(
        ...,
        description="The value to assign if the logical condition evaluates to true."
    )
    and_: Optional[List[Union['ReplaceLogicalExpression', ReplaceCondition]]] = Field(
        None,
        alias="and",
        description="List of conditions or nested expressions that must all be true (logical AND)."
    )
    or_: Optional[List[Union['ReplaceLogicalExpression', ReplaceCondition]]] = Field(
        None,
        alias="or",
        description="List of conditions or nested expressions where at least one must be true (logical OR)."
    )


class ReplaceExpression(BaseModel):
    else_: ReplaceResultValue = Field(
        ...,
        # alias="else",
        description="Fallback value to use if none of the 'when' conditions are satisfied."
    )
    when: List[ReplaceLogicalExpression] = Field(
        ...,
        description="Ordered list of conditional expressions evaluated for replacement. "
                    "The first matching expression determines the replacement value."
    )



class ReplaceAction(BaseModel):
    """Represents a replace action that applies a logical expression to replace dataset rows."""
    # action: Literal["filter"] = Field(..., description="Action type.")
    expression: ReplaceExpression = Field(..., description="Replace expression combining when, else and replace column fields.")
    replace_column: str = Field(
        ...,
        description="Name of the column whose values should be conditionally replaced."
    )