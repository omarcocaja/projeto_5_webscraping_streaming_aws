FROM public.ecr.aws/lambda/python:3.10

# Copia os arquivos da função
COPY app.py . 
COPY requirements.txt . 

# Instala as dependências
RUN pip install --upgrade pip \
    && pip install -r requirements.txt

# Define o handler
CMD ["app.lambda_handler"]
