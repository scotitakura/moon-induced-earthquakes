from earthquake_api import run_pipeline
from prefect.deployments import Deployment

def deploy():
    deployment = Deployment.build_from_flow(
        flow=run_pipeline,
        name="prefect-deployment"
    )
    deployment.apply()

if __name__ == "__main__":
    deploy()