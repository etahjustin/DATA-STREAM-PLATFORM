# DATA-STREAM-PLATFORM
Une plateforme Data Lakehouse temps réel conteneurisée, construite avec Kafka, Spark Streaming, Delta Lake (MinIO) et Streamlit


Real-Time Data Streaming & Lakehouse Platform
Ce dépôt contient l'implémentation End-to-End et fonctionnelle d'une plateforme de données moderne, conçue pour ingérer, traiter et visualiser des flux de données en temps réel.

L'objectif de ce projet était de construire une architecture scalable et résiliente capable de gérer une simulation de haute fréquence (10k événements/sec), en suivant les meilleures pratiques de l'industrie : le pattern Data Lakehouse.

🎯 Points Clés & Fonctionnalités
⚡ Pipeline Événementiel Complet : De la génération de données (Python Faker) à l'ingestion via Apache Kafka.

🏗️ Architecture Lakehouse Robuste : Utilisation de Spark Structured Streaming pour écrire des données en temps réel dans un format fiable et transactionnel (Delta Lake) sur du stockage objet S3 (MinIO). C'est la fondation de la "Bronze Layer".

✅ Gouvernance & Qualité de Donnée : Implémentation d'un Stream Processor intermédiaire pour valider les données à la volée et router les erreurs vers une Dead Letter Queue (DLQ), garantissant que seules les données propres atteignent le Lakehouse.

📊 Visualisation Temps Réel : Un dashboard Streamlit connecté directement au Lakehouse pour monitorer les KPIs d'ingestion.

🐳 Infrastructure as Code (IaC) : La stack entière (6 services) est conteneurisée et orchestrée via un unique fichier Docker Compose, permettant un déploiement local reproductible en une commande.
