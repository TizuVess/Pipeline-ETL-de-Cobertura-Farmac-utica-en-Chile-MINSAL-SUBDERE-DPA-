# Pipeline ETL de Cobertura Farmacéutica en Chile (MINSAL + SUBDERE DPA)

## 1. Requisitos e Inicialización de la Base de Datos
1. Crear una base de datos en PostgreSQL llamada `dw_salud_chile`.
2. Ejecutar el script DDL inicial para crear los esquemas (`raw`, `staging`, `dw`, `mart`) y las tablas:
   ```bash
   psql -h localhost -U postgres -d dw_salud_chile -f sql/01_ddl_esquemas_y_tablas.sql# Pipeline-ETL-de-Cobertura-Farmac-utica-en-Chile-MINSAL-SUBDERE-DPA-
