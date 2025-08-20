pipeline {
    agent {
        docker {
            image 'python:3.11-slim'
        }
    }

    environment {
        MONGO_URI = credentials('b2eada42-2521-45e0-8f40-072912c52410') 
        DATABASE_NAME = 'middelware' 
    }

    stages {
        stage('Instalar dependencias') {
            steps {
                sh 'pip install -r requirements.txt'
            }
        }

        stage('Validar conexión MongoDB') {
            steps {
                sh 'python check_mongo.py'
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
