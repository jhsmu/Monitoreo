pipeline {
    agent any

    environment {
        MONGO_URI = credentials('b2eada42-2521-45e0-8f40-072912c52410') 
        DATABASE_NAME = 'middelware' 
    }

    stages {
        stage('Preparar entorno') {
            steps {
                sh 'python3 -m venv venv'
                sh './venv/bin/pip install pymongo python-dotenv'
            }
        }

        stage('Validar conexión MongoDB') {
            steps {
                sh './venv/bin/python scripts/check_mongo.py'
            }
        }
    }

    post {
        success {
            echo "✅ Conexión a MongoDB Atlas exitosa"
        }
        failure {
            echo "❌ Pipeline falló: No se pudo conectar a MongoDB Atlas"
        }
    }
}