# Image de la Lambda de scoring (scikit-learn ne tient pas dans un zip Lambda).
# Construite depuis la RACINE du dépôt (pour inclure src/common/features.py) :
#   docker build --platform linux/amd64 --provenance=false -f src/scoring/Dockerfile -t smartcnc-scoring .
# (infra/smartcnc.py push-scoring fait tout : build, login ECR, push.)
FROM public.ecr.aws/lambda/python:3.12

# Mêmes versions que l'entraînement (requirements.txt, fixées par B).
RUN pip install --no-cache-dir "scikit-learn==1.5.2" "numpy<2.1"

COPY src/common/features.py ${LAMBDA_TASK_ROOT}/
COPY src/scoring/handler.py src/scoring/state_store.py ${LAMBDA_TASK_ROOT}/

CMD ["handler.lambda_handler"]
