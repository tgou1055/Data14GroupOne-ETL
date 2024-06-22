# my-data-pipeline

### Python

```bash
# 1. Install or update python to version 3.8
# 2. cd to the directory where requirements.txt is located
# 3. Optional: activate your virtualenv
# 4. Run the following command to install required python packages
pip3 install -r requirements.txt
```

### Serverless

```bash
# Install Serverless taking wsl on windows as example
# 1. Updates ubuntu
sudo apt-get update && sudo apt-get upgrade -y
# 2. Install node
sudo apt-get install curl -y
curl –sL https://deb.nodesource.com/setup_12.x | sudo -E bash -
sudo apt-get nodejs -y
# 3. Test node
node -v
# 4. Test npm
npm -v
# 5.1 Install the Serverless cli, if you do not have Serverless pre-installed
sudo npm install -g serverless
# 5.2 Update Serverless from a previous version of Serverless, if you already have installed Serverless
sudo npm update -g serverless
# 6 Check serverless framework version
serverless -v
```
```bash
# Install serverless plugins: prunes, python-requirements, dotenv and glue in one dev
# https://github.com/UnitedIncome/serverless-python-requirements
cd ${PROJECT_FOLDER}
sudo npm install serverless-prune-plugin serverless-python-requirements serverless-dotenv-plugin serverless-glue --save-dev
```