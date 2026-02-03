import os
from datetime import datetime
import subprocess
from pathlib import Path
import hashlib

import git

from mg.db.postgres_manager import PostgresManager
from mg.logging.logger_manager import LoggerManager


class CronManager:
    def __init__(self):
        self.process_name = f"cron_manager"
        self.script_name = os.path.basename(__file__)
        self.script_path = os.path.dirname(__file__)
        self.logger = LoggerManager(
            script_name=self.script_name,
            script_path=self.script_path,
            process_name=self.process_name,
            sport=None,
            database="defaultdb",
            schema="control",
        )
        self.logger.log_exceptions()
        self.postgres_manager = PostgresManager(
            "digital_ocean", "defaultdb", "control", return_logging=False
        )
        self.wrappers_dir = os.path.expanduser("~/Documents/cron_wrappers")

        # Define environment variables that should be preserved in wrappers
        # These will be checked and exported in each wrapper script
        self.env_vars_to_preserve = [
            # Database credentials
            "POSTGRES_USER",
            "POSTGRES_PW",
            "DB_USER",
            "DB_PASSWORD",
            "DB_HOST",
            "DB_PORT",
            # API keys and tokens
            "API_KEY",
            "API_SECRET",
            "AUTH_TOKEN",
            "ACCESS_TOKEN",
            # Application settings
            "APP_ENV",
            "DEBUG",
            "VERBOSITY",
            "LOG_LEVEL",
            # Python settings
            "PYTHONPATH",
            "PYTHON_ENV",
            "PYTHONUNBUFFERED",
            # Custom settings
            "PROJECT_ROOT",
            "DATA_DIR",
            "OUTPUT_DIR",
        ]

    def ensure_log_directories(self) -> None:
        """Create log directories if they don't exist"""
        base_log_dir = os.path.expanduser("~/Documents/cron_logs")
        subdirs = [
            "draftkings",
            "underdog",
            "oddsjam",
            "etr",
            "rotogrinders",
            "blitz",
            "fightodds",
            "ourlads",
            "bestball",
            "simsavant",
        ]

        for subdir in subdirs:
            Path(os.path.join(base_log_dir, subdir)).mkdir(parents=True, exist_ok=True)

        # Create wrappers directory
        Path(self.wrappers_dir).mkdir(parents=True, exist_ok=True)

    def setup_script_directory(self) -> str:
        """Create and return the scripts directory path"""
        scripts_dir = os.path.expanduser("~/Documents/cron_scripts")
        Path(scripts_dir).mkdir(parents=True, exist_ok=True)
        return scripts_dir

    def update_git_repo(self, repo_path, branch):
        """Update git repository to specified branch"""
        try:
            repo = git.Repo(repo_path)
            current = repo.active_branch.name

            # Fetch latest changes
            repo.remotes.origin.fetch()

            # If we're not on the correct branch, checkout and pull
            if current != branch:
                repo.git.checkout(branch)

            # Pull latest changes
            repo.remotes.origin.pull()

            return True
        except Exception as e:
            print(f"Error updating git repository {repo_path}: {str(e)}")
            return False

    def validate_cron_schedule(self, schedule):
        """Validate a cron schedule and return a fixed version if possible"""
        parts = schedule.split()

        if len(parts) != 5:
            return None, f"Schedule must have 5 parts, but has {len(parts)}"

        # Check each field individually
        fixed_parts = []
        fields = ["minute", "hour", "day_of_month", "month", "day_of_week"]

        # Define valid ranges for each field
        ranges = [
            (0, 59),  # minute
            (0, 23),  # hour
            (1, 31),  # day of month
            (1, 12),  # month
            (0, 7),  # day of week (0 and 7 are both Sunday)
        ]

        for i, part in enumerate(parts):
            # Skip validation for wildcards or step values
            if part == "*" or "/" in part:
                fixed_parts.append(part)
                continue

            # Check ranges and lists
            values = []
            for segment in part.split(","):
                if "-" in segment:
                    # Handle ranges like 1-5
                    try:
                        start, end = map(int, segment.split("-"))
                        if start < ranges[i][0] or end > ranges[i][1]:
                            return None, f"Invalid range {segment} for {fields[i]}"
                        values.extend([str(j) for j in range(start, end + 1)])
                    except ValueError:
                        return (
                            None,
                            f"Invalid range format in {segment} for {fields[i]}",
                        )
                else:
                    # Handle single values
                    try:
                        value = int(segment)
                        if value < ranges[i][0] or value > ranges[i][1]:
                            return None, f"Value {value} out of range for {fields[i]}"
                        values.append(str(value))
                    except ValueError:
                        return None, f"Invalid value {segment} for {fields[i]}"

            fixed_parts.append(",".join(values))

        return " ".join(fixed_parts), None

    def create_wrapper_script(self, command, log_path, description):
        """Create a shell wrapper script for a cron job and return its path"""
        # Create a name for the wrapper script based on the description
        safe_description = "".join(
            c if c.isalnum() else "_" for c in description
        ).strip("_")
        if not safe_description:
            command_hash = hashlib.md5(command.encode()).hexdigest()[:8]
            wrapper_name = f"cron_wrapper_{command_hash}.sh"
        else:
            command_hash = hashlib.md5(command.encode()).hexdigest()[:6]
            safe_description = safe_description[:30]
            wrapper_name = f"{safe_description}_{command_hash}.sh"

        wrapper_path = os.path.join(self.wrappers_dir, wrapper_name)

        # Check if command uses Conda environment
        conda_env = None
        if "/opt/anaconda3/envs/" in command:
            cmd_parts = command.split()
            for part in cmd_parts:
                if "/opt/anaconda3/envs/" in part:
                    env_path = part.split("/bin/")[0]
                    conda_env = os.path.basename(env_path)
                    break

        # Create macOS-compatible wrapper script
        wrapper_content = [
            "#!/bin/bash\n\n",
            "# Wrapper script generated by cron_manager\n",
            f"# Original command: {command}\n\n",
            "# Get user shell using macOS command\n",
            "USER_SHELL=$(dscl . -read /Users/$(whoami) UserShell | awk '{print $2}')\n\n",
            "# Load profile files\n",
            '[ -f "$HOME/.profile" ] && source "$HOME/.profile"\n',
            '[ -f "$HOME/.bash_profile" ] && source "$HOME/.bash_profile"\n',
            '[ -f "$HOME/.bashrc" ] && source "$HOME/.bashrc"\n\n',
            "# Load zsh profile files if that's the user's shell\n",
            'if [[ "$USER_SHELL" == *"zsh"* ]]; then\n',
            '  [ -f "$HOME/.zshenv" ] && source "$HOME/.zshenv"\n',
            '  [ -f "$HOME/.zprofile" ] && source "$HOME/.zprofile"\n',
            '  [ -f "$HOME/.zshrc" ] && source "$HOME/.zshrc"\n',
            "fi\n\n",
        ]

        # Add Conda environment activation if detected
        if conda_env:
            wrapper_content.extend(
                [
                    "# Initialize conda\n",
                    'if [ -f "/opt/anaconda3/etc/profile.d/conda.sh" ]; then\n',
                    '  . "/opt/anaconda3/etc/profile.d/conda.sh"\n',
                    "else\n",
                    '  export PATH="/opt/anaconda3/bin:$PATH"\n',
                    "fi\n\n",
                    f"# Activate conda environment: {conda_env}\n",
                    f'conda activate {conda_env} || echo "Failed to activate conda environment {conda_env}"\n\n',
                ]
            )

        # Set environment variables
        wrapper_content.extend(
            [
                "# Set critical environment variables\n",
                'export PATH="$HOME/.local/bin:$HOME/bin:/usr/local/bin:/usr/bin:/bin:$PATH"\n',
                'export SHELL="${USER_SHELL:-/bin/bash}"\n',
                'export LANG="${LANG:-en_US.UTF-8}"\n\n',
            ]
        )

        # Add environment variable exports
        for var in self.env_vars_to_preserve:
            wrapper_content.append(f'[ ! -z "${var}" ] && export {var}="${var}"\n')

        wrapper_content.append("\n")

        # Add the execution logic with macOS-compatible date handling
        wrapper_content.extend(
            [
                "# Record start time\n",
                'START_TIME=$(date +"%Y-%m-%d %H:%M:%S")\n',
                "START_SECONDS=$(date +%s)\n",
                'echo "[${START_TIME}] Starting cron job"\n',
                'echo "Command: ${0}"\n',
                'echo "User: $(whoami)"\n',
                'echo "Working Directory: $(pwd)"\n',
                'echo "Using Shell: $SHELL"\n',
                'echo "PATH: $PATH"\n',
                conda_env and f'echo "Conda Environment: {conda_env}"\n' or "",
                "\n# Set up error handling\n",
                "set -e\n\n",
                "# Execute the command and capture its exit status\n",
                "{\n",
                f"  {command}\n",
                "  EXIT_STATUS=$?\n",
                "} || {\n",
                "  EXIT_STATUS=$?\n",
                '  END_TIME=$(date +"%Y-%m-%d %H:%M:%S")\n',
                "  END_SECONDS=$(date +%s)\n",
                '  echo "[${END_TIME}] Command failed with exit status ${EXIT_STATUS}"\n',
                '  echo "Duration: $(( END_SECONDS - START_SECONDS )) seconds"\n',
                "  exit ${EXIT_STATUS}\n",
                "}\n\n",
                "# Record end time and duration\n",
                'END_TIME=$(date +"%Y-%m-%d %H:%M:%S")\n',
                "END_SECONDS=$(date +%s)\n",
                'echo "[${END_TIME}] Command completed successfully"\n',
                'echo "Duration: $(( END_SECONDS - START_SECONDS )) seconds"\n',
                "exit ${EXIT_STATUS}\n",
            ]
        )

        # Write the wrapper script
        with open(wrapper_path, "w") as f:
            f.writelines(wrapper_content)

        # Make it executable
        os.chmod(wrapper_path, 0o755)

        return wrapper_path

    def generate_cron_script(self) -> None:
        # Ensure log directories exist
        self.ensure_log_directories()

        # Setup scripts directory
        scripts_dir = self.setup_script_directory()

        # Get all active cron jobs
        cron_jobs = self.postgres_manager.execute(
            """
            SELECT schedule, command, git_repo_path, git_branch, log_path, description
            FROM cron_jobs
            WHERE is_active = true
            ORDER BY id;
            """
        )

        # Track unique repositories to update
        repos_updated = set()

        # Update git repositories first
        for job in cron_jobs:
            if job["git_repo_path"] and job["git_branch"]:
                repo_key = (job["git_repo_path"], job["git_branch"])
                if repo_key not in repos_updated:
                    if self.update_git_repo(job["git_repo_path"], job["git_branch"]):
                        repos_updated.add(repo_key)
                    else:
                        self.logger.log(
                            level="WARNING",
                            message=f"Failed to update repository {job['git_repo_path']} branch {job['git_branch']}",
                        )

        # First, remove all existing wrapper scripts to ensure we don't have outdated versions
        try:
            existing_wrappers = os.listdir(self.wrappers_dir)
            for wrapper in existing_wrappers:
                if wrapper.endswith(".sh"):
                    wrapper_path = os.path.join(self.wrappers_dir, wrapper)
                    os.remove(wrapper_path)
                    self.logger.log(
                        level="INFO",
                        message=f"Removed old wrapper script: {wrapper_path}",
                    )
        except Exception as e:
            self.logger.log(
                level="WARNING",
                message=f"Failed to clean up old wrapper scripts: {str(e)}",
            )

        # Generate shell script content
        script_content = [
            "#!/bin/bash\n",
            f"# Generated by cron manager on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n",
            "\n# Clear any existing cron jobs\n",
            "crontab -r\n\n",
            "# Ensure log directories exist\n",
            "mkdir -p ~/Documents/cron_logs/{draftkings,underdog,oddsjam,etr,rotogrinders,blitz,fightodds,ourlads,bestball,simsavant}\n",
            "mkdir -p ~/Documents/cron_wrappers\n\n",
        ]

        # Add each cron job, but validate the schedule first
        for i, job in enumerate(cron_jobs, 1):
            # Ensure log path ends with .txt
            log_path = os.path.expanduser(job["log_path"])
            if not log_path.endswith(".txt"):
                # Remove any existing extension and add .txt
                log_path = os.path.splitext(log_path)[0] + ".txt"

            # Validate and potentially fix the cron schedule
            fixed_schedule, error = self.validate_cron_schedule(job["schedule"])

            if error:
                self.logger.log(
                    level="WARNING",
                    message=f"Line {i}: Invalid cron schedule '{job['schedule']}': {error}. Skipping job.",
                )
                continue

            # Get description or use a default
            description = job.get("description", f"job_{i}")

            # Create a wrapper script for this job
            wrapper_path = self.create_wrapper_script(
                job["command"], log_path, description
            )

            # Use the fixed schedule with the wrapper script
            # Use > instead of >> to overwrite log file instead of appending
            script_content.append(
                f'(crontab -l ; echo "{fixed_schedule} {wrapper_path} > {log_path} 2>&1") | crontab -\n'
            )

        # Add final success message
        script_content.append('\necho "Cron jobs have been set up successfully!"\n')

        # Write to file in the scripts directory
        script_path = os.path.join(scripts_dir, "setup_cron_jobs.sh")
        with open(script_path, "w") as f:
            f.writelines(script_content)

        # Make script executable
        os.chmod(script_path, 0o755)

        # Execute the script
        try:
            subprocess.run([script_path], check=True)
            self.logger.log(
                level="INFO", message="Cron jobs have been set up successfully!"
            )
        except subprocess.CalledProcessError as e:
            self.logger.log(
                level="ERROR",
                message=f"Failed to install crontab: {str(e)}",
            )
            # Print the content of the script for debugging
            with open(script_path, "r") as f:
                self.logger.log(
                    level="DEBUG",
                    message=f"Script content:\n{f.read()}",
                )


def main() -> None:
    # Create the cron_scripts directory
    os.makedirs("cron_scripts", exist_ok=True)

    cron = CronManager()

    # Generate category-specific shell scripts
    cron.generate_cron_script()

    cron.postgres_manager.close()
    cron.logger.display_logs()
    cron.logger.close_logger()


if __name__ == "__main__":
    main()
