# Water Level Forecast

A machine learning pipeline that periodically retrieves official water level and precipitation data via REST APIs and applies Gaussian Processes to forecast Elbe river water levels, with results visualized on a web interface.

## About This Project

This is a personal learning project developed in my free time to gain hands-on experience and skills with new technologies. It combines various aspects of modern software development including:

- Machine Learning with Gaussian Processes
- REST API integration
- Data pipeline orchestration
- Web interface development
- Docker containerization
- Automated data retrieval and processing
- Database integration (InfluxDB for time series, PostGIS for geospatial data)
- Cloud deployment on a VM

## Technologies Used

- **Docker** - Containerization and deployment
- **Python** - Core programming language
- **Gaussian Processes** - Machine learning for forecasting
- **REST APIs** - Data retrieval from official sources
- **InfluxDB** - Time series data storage
- **PostGIS** - Geospatial data storage

## Data Sources

- **Pegelonline** - Official water level measurements from WSV (Wasserstraßen- und Schifffahrtsverwaltung des Bundes)
- **DWD** - Precipitation and weather forecast data from Deutscher Wetterdienst (German Weather Service)

## Project Status

### ✅ Done

- [x] Data extraction infrastructure with REST API clients
- [x] Schema validation for external data sources
- [x] Parsers for DWD weather data (KMZ, CSV formats)
- [x] Extractors for Pegelonline water level data
- [x] Docker setup with InfluxDB configuration
- [x] Pre-commit hooks for code quality

### 📋 To-Do

- [ ] Transformation of extracted data for loading into databases
- [ ] Loading transformed data into InfluxDB
- [ ] Create PostGIS schema for geospatial water level data
- [ ] Transform and load data into PostGIS
- [ ] Implement Gaussian Process model for water level forecasting
- [ ] Write transformations and loading for storing forecasts into databases
- [ ] Develop web interface for visualization
- [ ] Set up periodic data retrieval scheduling
- [ ] Add logging and monitoring

---

*This project serves as a practical learning experience to explore and gain proficiency in modern data science, machine learning, and software engineering technologies.*
