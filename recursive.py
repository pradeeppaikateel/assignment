import pandas as pd
import json


def recursive_pivot(data, headers, value_field):
    # Initialize the result dictionary
    result = {
        "header": "root",
        "field_name": "root",
        "value": "0",
        "children": []
    }

    # Group the data by the first header
    groups = data.groupby(headers[0])

    # Iterate over the groups
    for name, group in groups:
        # Calculate the total value of the group
        total_value = group[value_field].sum()

        # Create a new dictionary for the group
        group_dict = {
            "header": headers[0],
            "field_name": name,
            "value": str(total_value),
            "children": []
        }

        # If there are more headers, recursively pivot the group
        if len(headers) > 1:
            group_dict["children"] = recursive_pivot(group, headers[1:], value_field)

        # Add the group dictionary to the result
        result["children"].append(group_dict)

    # Return the result
    return result


# Define the input data
data = {
    "product": [
        "savings",
        "savings",
        "savings",
        "savings",
        "credit",
        "credit",
        "credit",
        "credit",
        "credit"
    ],
    "driver": [
        "all",
        "all",
        "branches",
        "branches",
        "all",
        "all",
        "branches",
        "branches",
        "support"
    ],
    "cust": [
        "a",
        "b",
        "c",
        "d",
        "e",
        "f",
        "g",
        "h",
        "i"
    ],
    "value": [
        1,
        2,
        3,
        4,
        5,
        6,
        7,
        8,
        9
    ]
}

# Convert the data to a Pandas DataFrame
df = pd.DataFrame(data)

# Define the headers and the value field
headers = ["product", "driver", "cust"]
value_field = "value"

# Pivot the data
result = recursive_pivot(df, headers, value_field)

# Calculate the total value of the root node
result["value"] = str(df[value_field].sum())

# Print the result
print(json.dumps(result))

with open(r'C:\Users\prade\Desktop\data.json', 'w') as outfile:
    # Write the dictionary to the file as JSON
    outfile.write(json.dumps(result))
