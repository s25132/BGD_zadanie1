# BGD_zadanie1

## Cel zadania 
Celem projektu jest zaprojektowanie i implementacja skalowalnego pipeline’u przetwarzania danych transakcyjnych, który przekształca surowe, potencjalnie błędne dane w wysokiej jakości model analityczny (warstwa GOLD), umożliwiający wiarygodne raportowanie i analizę biznesową.

## Dane
Dane posidają następujące kolumny 

- transaction_id – unikalny identyfikator transakcji
- customer_id – unikalny identyfikator klienta
- customer_name – imię i nazwisko klienta
- merchant_id – unikalny identyfikator sprzedawcy
- transaction_ts – znacznik czasu wykonania transakcji
- amount – kwota transakcji
- city – miasto sprzedawcy
- country – kraj sprzedawcy
- payment_method – metoda płatności
- status – status transakcji

Do repozytorium GitHub dołączone zostały przykładowe pliki danych (katalog data).
Pipeline był również testowany na dużym pliku CSV o rozmiarze ~2,5 GB.

## Pipeline 

Pipeline ma cztery moduły
###  RAW Ingestion – raw.py-> Cel: przechowywanie surowych danych

- Wczytuje dane z pliku CSV w partiach z katalogu data.

- Sprawdza, czy plik został już wcześniej załadowany.

- Ładuje tylko nowy plik, dzięki czemu możliwe jest dopisywanie nowych danych do już istniejących bez ponownego przetwarzania całości.

- Dodaje numer batcha (batch_no) dla każdej nowej partii danych.

- Zapisuje czyste dane do tabeli: raw.transactions_raw

- Rejestruje informację o załadowanym pliku w tabeli metadata, aby uniknąć wielokrotnego załadowania tego samego pliku.

### Cleaned and validated data – silver.py -> Cel: oczyszczone i zwalidowane dane

- Pobiera dane z warstwy RAW batchami

- Przetwarza tylko nowe batch’e, które nie zostały jeszcze zapisane w warstwie SILVER

- Czyści i normalizuje dane

- Wykonuje walidację

- Dodaje kolumny opisujące rezultat walidacji

- Zapisuje dane do: silver.transactions_clean

### Analytical Modeling – gold.py ->  Cel: dane gotowe do analizy i raportowania

Buduje model analityczny (schemat gwiazdy):

- Tabele wymiarów: gold.dim_customer, gold.dim_merchant, gold.dim_date

- Tabela faktów: gold.fact_transactions

### Pipeline control - pipeline.py

- steruje uruchamianiem wszystkich etapów przetwarzania danych (RAW, SILVER, GOLD).

Cechy:

- Uwzględnia tylko poprawne dane (is_valid = true)

- Zabezpieczenie przed duplikatami (ON CONFLICT DO NOTHING) 

- Tworzenie widoku dla raportu gold.v_transaction_report

- Umożliwia inkrementalne dopisywanie nowych danych do bazy

## Uruchomienie

Uruchomienie skrytpu pipeline 

docker compose -f pipeline_docker.yml build 

docker compose -f pipeline_docker.yml up 

Podgląd danych 

docker compose -f show_data.yml build 

docker compose -f show_data.yml up

## Sql tworzący bazę danych
BGD_zadanie1/pipeline/db/init.sql

## Przydatne sql
SELECT count(1) FROM "raw".transactions_raw

SELECT count(1) FROM silver.transactions_clean

select * from gold.fact_transactions limit 1000


SELECT 
    schemaname,
    relname AS table_name,
    pg_total_relation_size(relid) / 1024 / 1024 / 1024 AS size_gb,
	pg_total_relation_size(relid) / 1024 / 1024 AS mb_gb
FROM pg_catalog.pg_statio_user_tables
ORDER BY size_gb DESC

## Architektura
![GRAPH](BGD_zadanie11.png)