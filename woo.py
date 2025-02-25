import json
import os
import re
import subprocess
import tempfile

from bs4 import BeautifulSoup

def run_sqlmesh_dag():
    """Run sqlmesh dag command and save output to a temporary file"""
    try:
        with tempfile.NamedTemporaryFile(suffix='.html', delete=False) as tmp_file:
            temp_path = tmp_file.name

        subprocess.run(['sqlmesh', 'dag', temp_path], check=True)

        with open(temp_path, 'r') as f:
            content = f.read()

        os.unlink(temp_path)

        return content
    except subprocess.CalledProcessError as e:
        print(f"Error running sqlmesh dag: {e}")
        return None
    except Exception as e:
        print(f"Unexpected error: {e}")
        return None

def extract_graph_data(html_content) -> str:
    """Extract nodes and edges from the HTML output"""
    soup = BeautifulSoup(html_content, 'html.parser')

    script = soup.find('script', string=re.compile('vis.DataSet'))
    if not script:
        print("No script found with vis.DataSet")
        return None, None

    edges_match = re.search(r'edges: new vis\.DataSet\((.*?)\)', script.string, re.DOTALL)

    try:
        edges = json.loads(edges_match.group(1))
        return edges
    except json.JSONDecodeError as e:
        print(f"Error parsing JSON: {e}")
        return None

def get_db_and_schema_from_id(node_id):
    """Extract database and schema names from node ID"""
    parts = node_id.split('.')
    if len(parts) >= 3:
        return parts[0].strip('"'), parts[1].strip('"')
    return None, None

def get_schema_order(schema):
    """Helper function to determine schema order"""
    order = {
        'bronze': 0,
        'silver': 1,
        'gold': 2
    }
    return order.get(schema, 999)

def main():
    html_output = run_sqlmesh_dag()
    if not html_output:
        return

    rows = extract_graph_data(html_output)

    new_rows = []

    for row in rows:
        new_row = {}
        new_row["parent_name"] = row["from"]
        new_row["name"] = row["to"]
        new_rows.append(new_row)

    print(new_rows)

if __name__ == "__main__":
    main()