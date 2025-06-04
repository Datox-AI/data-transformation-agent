import os 
from pydantic import BaseModel, Field
from typing import Literal, List

from langchain_openai import AzureChatOpenAI
from langgraph.checkpoint.memory import MemorySaver
from langgraph.prebuilt import create_react_agent
from langchain_core.messages import (
    AIMessage,
    AnyMessage,
    BaseMessage,
    SystemMessage,
    ToolMessage,
)
from langgraph.graph import MessagesState, StateGraph, END
from langgraph.prebuilt import ToolNode
from dotenv import load_dotenv

from src.transformation_agent.tools import filter_tool, replace_tool, get_transformation_data, remove_last_step
from src.transformation_agent.prompts import system_prompt
load_dotenv(override=True)


class StructuredOutputResponse(BaseModel):
    """
    Pydantic model for structured response from the agent.
    This model defines the format of the final output that will be returned to the user.
    """
    answer: str = Field(description="Answer of agent")


class AgentState(MessagesState):
    """
    State class that extends MessagesState to maintain the conversation state.
    Includes both the message history and the final structured response.
    """
    # Final structured response from the agent
    final_response: StructuredOutputResponse


# List of available tools for the agent to use
tools = [get_transformation_data, remove_last_step, filter_tool, replace_tool, StructuredOutputResponse]

# Initialize the Azure OpenAI model with specific configuration
model = AzureChatOpenAI(
    azure_deployment="gpt-4o",
    api_version="2024-08-01-preview",
    temperature=0,
)
# Bind the tools to the model and force it to use tools
model_with_response_tool = model.bind_tools(tools, tool_choice="any")


def call_model(state: AgentState):
    """
    Calls the language model with the current state and system prompt.
    
    Args:
        state (AgentState): Current state containing message history
        
    Returns:
        dict: Updated messages with the model's response
    """
    system_prompt_message = SystemMessage(content=system_prompt)
    state["messages"].append(system_prompt_message)
    response = model_with_response_tool.invoke(state["messages"])
    print(response, " response")
    return {"messages": [response]}

def respond(state: AgentState):
    """
    Processes the final response from the agent and formats it for the user.
    
    Args:
        state (AgentState): Current state containing message history
        
    Returns:
        dict: Contains the final structured response and a tool message
    """
    # Construct the final answer from the arguments of the last tool call
    answer_tool_call = state["messages"][-1].tool_calls[0]
    response = StructuredOutputResponse(**answer_tool_call["args"])    # financial_tool_call = state["messages"][-1].tool_calls[0]
    # response = StructuredOutputListResponse(**financial_tool_call["args"])
    # Since we're using tool calling to return structured output,
    # we need to add  a tool message corresponding to the WeatherResponse tool call,
    # This is due to LLM providers' requirement that AI messages with tool calls
    # need to be followed by a tool message for each tool call
    tool_message = {
        "type": "tool",
        "content": "Here is your structured response",
        "tool_call_id": answer_tool_call["id"],
    }
    # We return the final answer
    return {"final_response": response, "messages": [tool_message]}

def should_continue(state: AgentState):
    """
    Determines the next step in the workflow based on the current state.
    
    Args:
        state (AgentState): Current state containing message history
        
    Returns:
        str: Either "respond" to end the conversation or "continue" to use tools
    """
    messages = state["messages"]
    last_message = messages[-1]
    print(last_message, " last_message")
    
    # Check if we have a final response ready
    if (
        len(last_message.tool_calls) == 1
        and last_message.tool_calls[0]["name"] == "StructuredOutputResponse"
    ):
        return "respond"
    # Otherwise continue with tool usage
    else:
        return "continue"

# Initialize the workflow graph
workflow = StateGraph(AgentState)

# Add nodes to the workflow
workflow.add_node("agent", call_model)  # Node for model interaction
workflow.add_node("respond", respond)   # Node for final response
workflow.add_node("tools", ToolNode(tools))  # Node for tool execution

# Set the entry point of the workflow
workflow.set_entry_point("agent")

# Define the workflow logic with conditional edges
workflow.add_conditional_edges(
    "agent",
    should_continue,
    {
        "continue": "tools",  # Continue to tools if more processing needed
        "respond": "respond", # Go to response if we have a final answer
    },
)
workflow.add_edge("tools", "agent")  # After tools, go back to agent
workflow.add_edge("respond", END)    # End the workflow after response

# Compile the workflow into an executable graph
graph = workflow.compile()