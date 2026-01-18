import requests
import json


class APIClient:
    def __init__(self, config_file="C:\GG\gglib\cfb\data\pff\config.json"):
        self.config_file = config_file
        self.load_config()

    def load_config(self):
        with open(self.config_file, "r") as file:
            self.config = json.load(file)
        self.headers = self.config["headers"]
        self.default_url = self.config["default_url"]

    def save_config(self):
        with open(self.config_file, "w") as file:
            json.dump(self.config, file, indent=4)

    def refresh_tokens(self):
        login_url = self.config["login"]["url"]
        login_payload = self.config["login"]["payload"]
        auth_headers = self.config["login"]["headers"]

        # Perform login to get new tokens
        response = requests.post(login_url, json=login_payload, headers=auth_headers)

        if response.status_code == 200:
            print("Login successful")
            # Print response headers and body for debugging
            print("Response Headers:", response.headers)
            print("Response Body:", response.text)

            # Extract the Authorization token from the headers
            access_token = response.headers.get("authorization")

            # Assuming the necessary cookies are included in the original headers
            cookies = response.cookies.get_dict()

            # Update headers with new tokens and cookies
            if access_token:
                self.headers["Authorization"] = access_token
            else:
                print("Failed to obtain the access token from the login response.")
                return False

            # Debugging prints
            print("Access Token:", access_token)
            print("Cookies:", cookies)

            # Assuming you need to keep the original cookies and update them if they are present in the response
            if cookies:
                cookie_header = self.headers.get("cookie", "")
                for key, value in cookies.items():
                    if f"{key}=" not in cookie_header:
                        cookie_header += f"; {key}={value}"
                self.headers["cookie"] = cookie_header

            # Save updated config
            self.config["headers"] = self.headers
            self.save_config()
            return True
        else:
            print(f"Login failed: {response.status_code} - {response.text}")
            return False

    def fetch_data(self, url=None):
        if url is None:
            url = self.default_url

        response = requests.get(url, headers=self.headers)

        print(f"Status Code: {response.status_code}")
        print(f"Response Text: {response.text}")

        if response.status_code == 200:
            data = response.json()
            print(data)
        else:
            print(f"Error: {response.status_code} - {response.text}")


if __name__ == "__main__":
    client = APIClient()
    if client.refresh_tokens():
        client.fetch_data()
