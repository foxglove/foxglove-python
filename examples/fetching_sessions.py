from foxglove.client import Client

token = "<YOUR API TOKEN HERE>"
project_id = "<YOUR PROJECT ID HERE>"

client = Client(token=token)

sessions = client.get_sessions(project_id=project_id)

for session in sessions:
    print(
        f"{session['id']} - {session['key']} - {len(session['recordings'])} recordings"
    )
