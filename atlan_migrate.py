from pyatlan.client.atlan import AtlanClient
import os
import json
import requests
import time
from dotenv import load_dotenv
from typing import Dict, List, Any


def build_members_mod_rover_payload(list_additions: List[str], list_deletions: List[str]) -> Dict[str, Any]:
    try:
        payload = {}

        if list_additions:
            payload["additions"] = [{"type": "user", "id": user_id} for user_id in list_additions]
        else:
            payload["additions"] = []

        if list_deletions:
            payload["deletions"] = [{"type": "user", "id": user_id} for user_id in list_deletions]
        else:
            payload["deletions"] = []
        return payload

    except Exception as e:
        print(f"Error building payload: {str(e)}")
        return {}


def save_to_json(data: Dict, filename: str) -> None:
    with open(f'{filename}.json', 'w') as f:
        json.dump(data, f, indent=2, default=str)


def load_from_json(filename: str) -> Dict:
    with open(f'{filename}.json', 'r') as f:
        return json.load(f)


class AtlanMigrationClient:
    def __init__(self):
        load_dotenv()
        self.base_url = os.getenv("ATLAN_BASE_URL")
        self.api_key = os.getenv("ATLAN_API_KEY")
        self.rover_url = os.getenv("ATLAN_ROVER_URL")
        
        if not self.base_url or not self.api_key:
            raise ValueError("ATLAN_BASE_URL and ATLAN_API_KEY must be set in environment")
        
        self.client = AtlanClient(
            base_url=self.base_url,
            api_key=self.api_key
        )
    
    def fetch_all_users(self) -> Dict[str, Dict[str, Any]]:
        users = self.client.user.get_all()
        
        all_atlan_users = {}
        for user in users:
            all_atlan_users[user.username] = {
                "user_id": user.id,
                "email": user.email,
                "roles": user.roles,
                "personas": user.personas,
                "groups": user.group_count
            }
        
        return all_atlan_users
    
    def fetch_all_groups(self, limit: int = 100, offset: int = 1) -> Dict[str, Dict[str, Any]]:
        groups = self.client.group.get_all(
            limit=limit,
            offset=offset,
            sort="createdAt",
            columns=["roles", "path"]
        )
        
        all_atlan_groups = {}
        for group in groups:
            all_atlan_groups[group.name] = {
                "group_id": group.id,
                "group_alias": group.alias,
                "group_personas": group.roles,
            }
        
        return all_atlan_groups

    def update_users_cache(self) -> int:
        users = self.fetch_all_users()
        save_to_json(users, 'atlan_users')
        print(f"Updated atlan_users.json with {len(users)} users")
        return len(users)
    
    def update_groups_cache(self) -> int:
        groups = self.fetch_all_groups()
        save_to_json(groups, 'atlan_groups')
        print(f"Updated atlan_groups.json with {len(groups)} groups")
        return len(groups)

    def add_users_to_atlan_group(self, group_id: str, user_ids: List[str]) -> requests.Response:

        url = f"{self.base_url}/api/service/groups/{group_id}/members"

        headers = {
            'accept': 'application/json, text/plain, */*',
            'authorization': f'Bearer {self.api_key}',
            'Content-Type': 'application/json'
        }
        data = {
            "users": user_ids
        }
        
        response = requests.post(url, headers=headers, json=data)
        
        if response.status_code == 200:
            print(f"Successfully added {len(user_ids)} users to group {group_id}")
        else:
            print(f"Failed to add users to group. Status: {response.status_code}, Response: {response.text}")
        
        return response

    def batch_add_users_to_atlan_group(self, group_name: str, json_filename: str, batch_size: int = 20, delay: int = 5) -> bool:
        try:
            groups_data = load_from_json('atlan_groups')
            
            if group_name not in groups_data:
                print(f"Error: {group_name} group not found in groups")
                return False
            
            target_group_id = groups_data[group_name]["group_id"]
            print(f"Adding users to group: {group_name} (ID: {target_group_id})")
            
            users_data = load_from_json(json_filename)
            total_user_ids = [user_info["user_id"] for user_info in users_data.values()]
            total_users = len(total_user_ids)
            print(f"Total users to add: {total_users}")
            
            successful_batches = 0
            failed_batches = 0
            
            for i in range(0, total_users, batch_size):
                batch_user_ids = total_user_ids[i:i + batch_size]
                batch_num = (i // batch_size) + 1
                
                print(f"Processing batch {batch_num}: Adding {len(batch_user_ids)} users")
                
                response = self.add_users_to_atlan_group(target_group_id, batch_user_ids)
                
                if response.status_code == 200:
                    print(f"✓ Batch {batch_num} successful")
                    successful_batches += 1
                else:
                    print(f"✗ Batch {batch_num} failed")
                    failed_batches += 1
                
                if i + batch_size < total_users:
                    print(f"Waiting {delay} seconds before next batch...")
                    time.sleep(delay)
            
            print(f"Completed: {successful_batches} successful, {failed_batches} failed batches")
            return failed_batches == 0
            
        except Exception as e:
            print(f"Error in batch operation: {str(e)}")
            return False

    def add_users_to_rover_group(self, group_name: str, json_filename: str) -> bool:
        """(placeholder implementation)"""
        try:
            users_data = load_from_json(json_filename)
            total_user_ids = list(users_data.keys())
            total_users = len(total_user_ids)
            print(f"Total users to add: {total_users}")
            
            print(f"Would add {total_users} users to Rover group: {group_name}")
            print("Note: Rover integration not yet implemented")

            rover_url = f"{self.rover_url}/v1/{group_name}/membersMod"

            headers = {
                'accept': 'application/json',
                'authorization': f'Bearer {self.api_key}', # need to add rover creds here
                'Content-Type': 'application/json'
            }
            data = build_members_mod_rover_payload(total_user_ids, [])
            print(data)
            response = requests.post(rover_url, headers=headers, json=data)

            if response.status_code == 200:
                print(f"Successfully added {len(total_user_ids)} users to group {group_name}")
            else:
                print(f"Failed to add users to group. Status: {response.status_code}, Response: {response.text}")

            return True
        except Exception as e:
            print(f"Error processing Rover group operation: {str(e)}")
            return True


def main():
    migration_client = AtlanMigrationClient()
    
    migration_client.update_users_cache()
    migration_client.update_groups_cache()
    
    atlan_group_update = migration_client.batch_add_users_to_atlan_group("data_users", "atlan_users")

    rover_group_update = migration_client.add_users_to_rover_group("dataverse-atlan-users", "atlan_users")

    if atlan_group_update and rover_group_update:
        print("Migration completed successfully")
    else:
        print("Migration completed with some failures")


if __name__ == "__main__":

    main()