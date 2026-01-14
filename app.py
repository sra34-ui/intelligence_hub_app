import os
from flask import Flask, render_template, request, jsonify, session
from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config
import uuid

app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET_KEY", "skyscanner-intelligence-hub-secret")

# Disable caching for all responses
@app.after_request
def add_header(response):
    response.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate, post-check=0, pre-check=0, max-age=0'
    response.headers['Pragma'] = 'no-cache'
    response.headers['Expires'] = '-1'
    return response

# Initialize Databricks Workspace Client
# When running as Databricks App, uses provided service principal credentials
# When running locally, uses the default profile from ~/.databrickscfg
if os.environ.get("DATABRICKS_HOST") and os.environ.get("DATABRICKS_CLIENT_ID"):
    # Running in Databricks Apps environment
    config = Config(
        host=os.environ.get("DATABRICKS_HOST"),
        client_id=os.environ.get("DATABRICKS_CLIENT_ID"),
        client_secret=os.environ.get("DATABRICKS_CLIENT_SECRET")
    )
    w = WorkspaceClient(config=config)
else:
    # Running locally - use default profile
    w = WorkspaceClient()

# Multi-agent supervisor endpoint name
ENDPOINT_NAME = os.environ.get("DATABRICKS_SERVING_ENDPOINT", "mas-0359371c-endpoint")

# Data catalog and schema configuration
CATALOG = os.environ.get("CATALOG", "users")
SCHEMA = os.environ.get("SCHEMA", "sultan_alawar")


# ============================================================================
# Main Application Routes
# ============================================================================

@app.route('/')
def index():
    """Render the main chat interface"""
    if 'session_id' not in session:
        session['session_id'] = str(uuid.uuid4())
    return render_template('index.html')


@app.route('/dashboards/flights')
def flights_dashboard():
    """Render the Flights Intelligence dashboard"""
    return render_template('dashboards/flights.html')


@app.route('/dashboards/hotels')
def hotels_dashboard():
    """Render the Hotel Intelligence dashboard"""
    return render_template('dashboards/hotels.html')


@app.route('/dashboards/packages')
def packages_dashboard():
    """Render the Packages Intelligence dashboard"""
    return render_template('dashboards/packages.html')


@app.route('/dashboards/reviews')
def reviews_dashboard():
    """Render the Customer Reviews Intelligence dashboard"""
    return render_template('dashboards/reviews.html')


@app.route('/ai-chat')
def ai_chat():
    """Render the AI Chat page"""
    if 'session_id' not in session:
        session['session_id'] = str(uuid.uuid4())
    return render_template('ai_chat.html')

@app.route('/travel-trends')
def travel_trends():
    """Render the Travel Trends page"""
    return render_template('travel_trends.html')


@app.route('/data-access')
def data_access():
    """Render the Data Access page"""
    return render_template('data_access.html')


# Simple cache for flight stats
_flight_stats_cache = None
_flight_stats_cache_time = None

# Simple cache for package stats
_package_stats_cache = None
_package_stats_cache_time = None

# Simple cache for review stats
_review_stats_cache = None
_review_stats_cache_time = None

@app.route('/api/flights/stats', methods=['GET'])
def get_flight_stats():
    """Get flight statistics from Unity Catalog synced_flights table"""
    global _flight_stats_cache, _flight_stats_cache_time

    # Return cached data if less than 10 minutes old
    import time as time_module
    if _flight_stats_cache and _flight_stats_cache_time:
        age = time_module.time() - _flight_stats_cache_time
        if age < 600:  # 10 minutes (extended from 5)
            print(f"DEBUG: Returning cached flight stats (age: {age:.1f}s)")
            response = jsonify(_flight_stats_cache)
            response.headers['Cache-Control'] = 'public, max-age=600'
            return response

    try:
        print("DEBUG: Fetching fresh flight stats from database...")

        # Get warehouse ID from environment variable or find a running one
        warehouse_id = os.environ.get("DATABRICKS_WAREHOUSE_ID")
        if not warehouse_id:
            warehouses = list(w.warehouses.list())
            for wh in warehouses:
                if wh.state.value == 'RUNNING':
                    warehouse_id = wh.id
                    break
            if not warehouse_id:
                raise Exception("No running SQL warehouse found")

        # Run separate simple queries (faster than one complex UNION ALL query)

        # Query 1: Top airlines
        airlines_result = w.statement_execution.execute_statement(
            warehouse_id=warehouse_id,
            catalog=CATALOG,
            schema=SCHEMA,
            statement=f"""
                SELECT
                    airline,
                    COUNT(*) as flight_count,
                    AVG(price) as avg_price,
                    AVG(duration_minutes) as avg_duration
                FROM {CATALOG}.{SCHEMA}.synced_flights
                WHERE airline IS NOT NULL
                GROUP BY airline
                ORDER BY COUNT(*) DESC
                LIMIT 10
            """
        ).result()

        airlines = []
        if airlines_result and airlines_result.data_array:
            for row in airlines_result.data_array:
                airlines.append({
                    'airline': row[0],
                    'flight_count': int(row[1]) if row[1] else 0,
                    'avg_price': float(row[2]) if row[2] else 0,
                    'avg_duration': int(float(row[3])) if row[3] else 0
                })

        # Query 2: Top routes
        routes_result = w.statement_execution.execute_statement(
            warehouse_id=warehouse_id,
            catalog=CATALOG,
            schema=SCHEMA,
            statement=f"""
                SELECT
                    origin,
                    destination,
                    COUNT(*) as flight_count,
                    AVG(price) as avg_price,
                    MIN(price) as min_price
                FROM {CATALOG}.{SCHEMA}.synced_flights
                WHERE origin IS NOT NULL AND destination IS NOT NULL
                GROUP BY origin, destination
                ORDER BY COUNT(*) DESC
                LIMIT 10
            """
        ).result()

        routes = []
        if routes_result and routes_result.data_array:
            for row in routes_result.data_array:
                routes.append({
                    'origin': row[0],
                    'destination': row[1],
                    'flight_count': int(row[2]) if row[2] else 0,
                    'avg_price': float(row[3]) if row[3] else 0,
                    'min_price': float(row[4]) if row[4] else 0
                })

        # Query 3: Cabin classes
        cabin_result = w.statement_execution.execute_statement(
            warehouse_id=warehouse_id,
            catalog=CATALOG,
            schema=SCHEMA,
            statement=f"""
                SELECT
                    cabin_class,
                    AVG(price) as avg_price,
                    COUNT(*) as count
                FROM {CATALOG}.{SCHEMA}.synced_flights
                WHERE cabin_class IS NOT NULL AND price IS NOT NULL
                GROUP BY cabin_class
            """
        ).result()

        cabin_classes = []
        if cabin_result and cabin_result.data_array:
            for row in cabin_result.data_array:
                cabin_classes.append({
                    'cabin_class': row[0],
                    'avg_price': float(row[1]) if row[1] else 0,
                    'count': int(row[2]) if row[2] else 0
                })

        # Query 4: Stops analysis
        stops_result = w.statement_execution.execute_statement(
            warehouse_id=warehouse_id,
            catalog=CATALOG,
            schema=SCHEMA,
            statement=f"""
                SELECT
                    stops,
                    COUNT(*) as count,
                    AVG(price) as avg_price,
                    AVG(duration_minutes) as avg_duration
                FROM {CATALOG}.{SCHEMA}.synced_flights
                WHERE stops IS NOT NULL
                GROUP BY stops
                ORDER BY stops
            """
        ).result()

        stops = []
        if stops_result and stops_result.data_array:
            for row in stops_result.data_array:
                stops.append({
                    'stops': int(row[0]) if row[0] is not None else 0,
                    'count': int(row[1]) if row[1] else 0,
                    'avg_price': float(row[2]) if row[2] else 0,
                    'avg_duration': int(float(row[3])) if row[3] else 0
                })

        # Query 5: Overall statistics
        overall_result = w.statement_execution.execute_statement(
            warehouse_id=warehouse_id,
            catalog=CATALOG,
            schema=SCHEMA,
            statement=f"""
                SELECT
                    COUNT(*) as total_flights,
                    AVG(price) as avg_price,
                    AVG(duration_minutes) as avg_duration,
                    AVG(available_seats) as avg_available_seats
                FROM {CATALOG}.{SCHEMA}.synced_flights
                WHERE price IS NOT NULL
            """
        ).result()

        overall = {
            'total_flights': 0,
            'avg_price': 0,
            'avg_duration': 0,
            'avg_available_seats': 0
        }
        if overall_result and overall_result.data_array and len(overall_result.data_array) > 0:
            row = overall_result.data_array[0]
            overall = {
                'total_flights': int(row[0]) if row[0] else 0,
                'avg_price': float(row[1]) if row[1] else 0,
                'avg_duration': int(float(row[2])) if row[2] else 0,
                'avg_available_seats': int(float(row[3])) if row[3] else 0
            }

        response_data = {
            'airlines': airlines,
            'routes': routes,
            'cabin_classes': cabin_classes,
            'stops': stops,
            'overall': overall
        }

        # Cache the result
        _flight_stats_cache = response_data
        _flight_stats_cache_time = time_module.time()

        print("DEBUG: Flight stats fetched and cached successfully")
        response = jsonify(response_data)
        response.headers['Cache-Control'] = 'public, max-age=600'
        return response

    except Exception as e:
        import traceback
        error_details = traceback.format_exc()
        print(f"ERROR: Exception in flight stats endpoint: {error_details}")
        app.logger.error(f"Error in flight stats endpoint: {str(e)}\n{error_details}")

        # Return mock data as fallback if query fails
        return jsonify({
            'airlines': [
                {'airline': 'United', 'flight_count': 150, 'avg_price': 450.50, 'avg_duration': 180},
                {'airline': 'Delta', 'flight_count': 142, 'avg_price': 425.75, 'avg_duration': 175},
                {'airline': 'American Airlines', 'flight_count': 135, 'avg_price': 410.25, 'avg_duration': 170},
                {'airline': 'Lufthansa', 'flight_count': 128, 'avg_price': 520.00, 'avg_duration': 240},
                {'airline': 'Air France', 'flight_count': 120, 'avg_price': 485.50, 'avg_duration': 220}
            ],
            'routes': [
                {'origin': 'JFK', 'destination': 'LAX', 'flight_count': 45, 'avg_price': 350.00, 'min_price': 250.00},
                {'origin': 'LHR', 'destination': 'CDG', 'flight_count': 40, 'avg_price': 180.00, 'min_price': 120.00},
                {'origin': 'ORD', 'destination': 'SFO', 'flight_count': 38, 'avg_price': 320.00, 'min_price': 220.00},
                {'origin': 'ATL', 'destination': 'MIA', 'flight_count': 35, 'avg_price': 280.00, 'min_price': 180.00},
                {'origin': 'DXB', 'destination': 'LHR', 'flight_count': 32, 'avg_price': 650.00, 'min_price': 450.00}
            ],
            'cabin_classes': [
                {'cabin_class': 'First', 'avg_price': 1200.00, 'count': 150},
                {'cabin_class': 'Business', 'avg_price': 800.00, 'count': 250},
                {'cabin_class': 'Premium Economy', 'avg_price': 450.00, 'count': 200},
                {'cabin_class': 'Economy', 'avg_price': 250.00, 'count': 400}
            ],
            'stops': [
                {'stops': 0, 'count': 600, 'avg_price': 420.00, 'avg_duration': 180},
                {'stops': 1, 'count': 300, 'avg_price': 320.00, 'avg_duration': 280},
                {'stops': 2, 'count': 100, 'avg_price': 250.00, 'avg_duration': 380}
            ],
            'overall': {
                'total_flights': 1000,
                'avg_price': 385.50,
                'avg_duration': 215,
                'avg_available_seats': 120
            },
            'error': f'Query failed, showing mock data: {str(e)}'
        })


@app.route('/api/packages/stats', methods=['GET'])
def get_package_stats():
    """Get package statistics from Unity Catalog synced_packages table"""
    global _package_stats_cache, _package_stats_cache_time

    # Return cached data if less than 10 minutes old
    import time as time_module
    if _package_stats_cache and _package_stats_cache_time:
        age = time_module.time() - _package_stats_cache_time
        if age < 600:  # 10 minutes (extended from 5)
            print(f"DEBUG: Returning cached package stats (age: {age:.1f}s)")
            response = jsonify(_package_stats_cache)
            response.headers['Cache-Control'] = 'public, max-age=600'
            return response

    try:
        print("DEBUG: Fetching fresh package stats from database...")

        # Use a single query with multiple CTEs for better performance
        combined_query = f"""
        WITH type_stats AS (
            SELECT
                package_type,
                COUNT(*) as package_count,
                AVG(final_price) as avg_price,
                AVG(duration_days) as avg_duration,
                ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC) as rn
            FROM {CATALOG}.{SCHEMA}.synced_packages
            WHERE package_type IS NOT NULL
            GROUP BY package_type
        ),
        destination_stats AS (
            SELECT
                destination,
                COUNT(*) as package_count,
                AVG(final_price) as avg_price,
                MIN(final_price) as min_price,
                ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC) as rn
            FROM {CATALOG}.{SCHEMA}.synced_packages
            WHERE destination IS NOT NULL
            GROUP BY destination
        ),
        route_stats AS (
            SELECT
                departure_city,
                destination,
                COUNT(*) as package_count,
                AVG(final_price) as avg_price,
                ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC) as rn
            FROM {CATALOG}.{SCHEMA}.synced_packages
            WHERE departure_city IS NOT NULL AND destination IS NOT NULL
            GROUP BY departure_city, destination
        ),
        duration_stats AS (
            SELECT
                CASE
                    WHEN duration_days <= 3 THEN '1-3 days'
                    WHEN duration_days <= 7 THEN '4-7 days'
                    WHEN duration_days <= 14 THEN '8-14 days'
                    ELSE '15+ days'
                END as duration_range,
                COUNT(*) as count,
                AVG(final_price) as avg_price,
                AVG(duration_days) as avg_days
            FROM {CATALOG}.{SCHEMA}.synced_packages
            WHERE duration_days IS NOT NULL
            GROUP BY duration_range
        ),
        overall_stats AS (
            SELECT
                COUNT(*) as total_packages,
                AVG(final_price) as avg_price,
                AVG(duration_days) as avg_duration,
                AVG(discount_percentage) as avg_discount
            FROM {CATALOG}.{SCHEMA}.synced_packages
            WHERE final_price IS NOT NULL
        )
        SELECT
            'types' as stat_type,
            package_type as name,
            package_count,
            avg_price,
            avg_duration,
            NULL as min_price,
            NULL as destination,
            NULL as departure_city,
            NULL as duration_range,
            NULL as count,
            NULL as avg_days,
            NULL as total_packages,
            NULL as avg_discount
        FROM type_stats
        UNION ALL
        SELECT
            'destinations',
            destination,
            package_count,
            avg_price,
            NULL,
            min_price,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL
        FROM destination_stats WHERE rn <= 10
        UNION ALL
        SELECT
            'routes',
            NULL,
            package_count,
            avg_price,
            NULL,
            NULL,
            destination,
            departure_city,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL
        FROM route_stats WHERE rn <= 10
        UNION ALL
        SELECT
            'durations',
            NULL,
            NULL,
            avg_price,
            NULL,
            NULL,
            NULL,
            NULL,
            duration_range,
            count,
            avg_days,
            NULL,
            NULL
        FROM duration_stats
        UNION ALL
        SELECT
            'overall',
            NULL,
            NULL,
            avg_price,
            avg_duration,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            total_packages,
            avg_discount
        FROM overall_stats
        """

        # Execute single combined query
        import time

        # Find a running warehouse
        warehouses = list(w.warehouses.list())
        warehouse_id = None
        for wh in warehouses:
            if wh.state.value == 'RUNNING':
                warehouse_id = wh.id
                break

        if not warehouse_id:
            raise Exception("No running SQL warehouse found")

        # Execute and wait for completion
        result = w.statement_execution.execute_statement(
            warehouse_id=warehouse_id,
            catalog=CATALOG,
            schema=SCHEMA,
            statement=combined_query
        )

        # Wait for completion and get results
        data = w.statement_execution.wait_get_statement_result_chunk_n(
            statement_id=result.statement_id,
            chunk_index=0
        )

        # Parse results
        package_types = []
        destinations = []
        routes = []
        durations = []
        overall = {
            'total_packages': 0,
            'avg_price': 0,
            'avg_duration': 0,
            'avg_discount': 0
        }

        if data and data.data_array:
            for row in data.data_array:
                row_type = row[0]

                if row_type == 'types':
                    package_types.append({
                        'package_type': row[1],
                        'package_count': int(row[2]) if row[2] else 0,
                        'avg_price': float(row[3]) if row[3] else 0,
                        'avg_duration': int(float(row[4])) if row[4] else 0
                    })
                elif row_type == 'destinations':
                    destinations.append({
                        'destination': row[1],
                        'package_count': int(row[2]) if row[2] else 0,
                        'avg_price': float(row[3]) if row[3] else 0,
                        'min_price': float(row[5]) if row[5] else 0
                    })
                elif row_type == 'routes':
                    routes.append({
                        'departure_city': row[7],
                        'destination': row[6],
                        'package_count': int(row[2]) if row[2] else 0,
                        'avg_price': float(row[3]) if row[3] else 0
                    })
                elif row_type == 'durations':
                    durations.append({
                        'duration_range': row[8],
                        'count': int(row[9]) if row[9] else 0,
                        'avg_price': float(row[3]) if row[3] else 0,
                        'avg_days': float(row[10]) if row[10] else 0
                    })
                elif row_type == 'overall':
                    overall = {
                        'total_packages': int(row[11]) if row[11] else 0,
                        'avg_price': float(row[3]) if row[3] else 0,
                        'avg_duration': int(float(row[4])) if row[4] else 0,
                        'avg_discount': float(row[12]) if row[12] else 0
                    }

        response_data = {
            'package_types': package_types,
            'destinations': destinations,
            'routes': routes,
            'durations': durations,
            'overall': overall
        }

        # Cache the result
        _package_stats_cache = response_data
        _package_stats_cache_time = time_module.time()

        print("DEBUG: Package stats fetched and cached successfully")
        response = jsonify(response_data)
        response.headers['Cache-Control'] = 'public, max-age=600'
        return response

    except Exception as e:
        import traceback
        error_details = traceback.format_exc()
        print(f"ERROR: Exception in package stats endpoint: {error_details}")
        app.logger.error(f"Error in package stats endpoint: {str(e)}\n{error_details}")

        # Return mock data as fallback if query fails
        return jsonify({
            'package_types': [
                {'package_type': 'Flight + Hotel', 'package_count': 450, 'avg_price': 1250.00, 'avg_duration': 7},
                {'package_type': 'Flight + Hotel + Car', 'package_count': 320, 'avg_price': 1580.00, 'avg_duration': 10},
                {'package_type': 'Beach Holiday', 'package_count': 280, 'avg_price': 2100.00, 'avg_duration': 14},
                {'package_type': 'City Break', 'package_count': 250, 'avg_price': 850.00, 'avg_duration': 4}
            ],
            'destinations': [
                {'destination': 'Dubai', 'package_count': 120, 'avg_price': 1800.00, 'min_price': 950.00},
                {'destination': 'Paris', 'package_count': 110, 'avg_price': 1200.00, 'min_price': 680.00},
                {'destination': 'Barcelona', 'package_count': 95, 'avg_price': 980.00, 'min_price': 550.00},
                {'destination': 'Tokyo', 'package_count': 85, 'avg_price': 2500.00, 'min_price': 1800.00},
                {'destination': 'New York', 'package_count': 80, 'avg_price': 1900.00, 'min_price': 1200.00}
            ],
            'routes': [
                {'departure_city': 'London', 'destination': 'Dubai', 'package_count': 45, 'avg_price': 1750.00},
                {'departure_city': 'Paris', 'destination': 'Barcelona', 'package_count': 38, 'avg_price': 890.00},
                {'departure_city': 'London', 'destination': 'Paris', 'package_count': 35, 'avg_price': 1100.00}
            ],
            'durations': [
                {'duration_range': '1-3 days', 'count': 250, 'avg_price': 650.00, 'avg_days': 2.5},
                {'duration_range': '4-7 days', 'count': 420, 'avg_price': 1200.00, 'avg_days': 5.8},
                {'duration_range': '8-14 days', 'count': 280, 'avg_price': 2100.00, 'avg_days': 10.5},
                {'duration_range': '15+ days', 'count': 50, 'avg_price': 3500.00, 'avg_days': 18.2}
            ],
            'overall': {
                'total_packages': 1000,
                'avg_price': 1485.50,
                'avg_duration': 8,
                'avg_discount': 15.5
            },
            'error': f'Query failed, showing mock data: {str(e)}'
        })


@app.route('/api/reviews/stats', methods=['GET'])
def get_review_stats():
    """Get review statistics from Unity Catalog synced_reviews table"""
    global _review_stats_cache, _review_stats_cache_time

    # Return cached data if less than 10 minutes old
    import time as time_module
    if _review_stats_cache and _review_stats_cache_time:
        age = time_module.time() - _review_stats_cache_time
        if age < 600:  # 10 minutes (extended from 5)
            print(f"DEBUG: Returning cached review stats (age: {age:.1f}s)")
            response = jsonify(_review_stats_cache)
            response.headers['Cache-Control'] = 'public, max-age=600'
            return response

    try:
        print("DEBUG: Fetching fresh review stats from database...")

        # Use a single query with multiple CTEs for better performance
        combined_query = f"""
        WITH rating_dist AS (
            SELECT
                rating,
                COUNT(*) as count,
                AVG(helpful_votes) as avg_helpful
            FROM {CATALOG}.{SCHEMA}.synced_reviews
            WHERE rating IS NOT NULL
            GROUP BY rating
        ),
        item_type_stats AS (
            SELECT
                item_type,
                COUNT(*) as review_count,
                AVG(rating) as avg_rating,
                SUM(CASE WHEN would_recommend = true THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as recommend_pct
            FROM {CATALOG}.{SCHEMA}.synced_reviews
            WHERE item_type IS NOT NULL
            GROUP BY item_type
        ),
        company_stats AS (
            SELECT
                company_name,
                COUNT(*) as review_count,
                AVG(rating) as avg_rating,
                ROW_NUMBER() OVER (ORDER BY AVG(rating) DESC, COUNT(*) DESC) as rn
            FROM {CATALOG}.{SCHEMA}.synced_reviews
            WHERE company_name IS NOT NULL
            GROUP BY company_name
        ),
        traveler_stats AS (
            SELECT
                traveler_type,
                COUNT(*) as count,
                AVG(rating) as avg_rating
            FROM {CATALOG}.{SCHEMA}.synced_reviews
            WHERE traveler_type IS NOT NULL
            GROUP BY traveler_type
        ),
        sentiment_stats AS (
            SELECT
                CASE
                    WHEN rating >= 4 THEN 'Positive'
                    WHEN rating = 3 THEN 'Neutral'
                    ELSE 'Negative'
                END as sentiment,
                COUNT(*) as count
            FROM {CATALOG}.{SCHEMA}.synced_reviews
            WHERE rating IS NOT NULL
            GROUP BY sentiment
        ),
        overall_stats AS (
            SELECT
                COUNT(*) as total_reviews,
                AVG(rating) as avg_rating,
                SUM(CASE WHEN verified_purchase = true THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as verified_pct,
                SUM(CASE WHEN would_recommend = true THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as recommend_pct
            FROM {CATALOG}.{SCHEMA}.synced_reviews
        )
        SELECT
            'ratings' as stat_type,
            CAST(rating as STRING) as name,
            count,
            avg_helpful,
            NULL as review_count,
            NULL as avg_rating,
            NULL as recommend_pct,
            NULL as item_type,
            NULL as company_name,
            NULL as traveler_type,
            NULL as sentiment,
            NULL as total_reviews,
            NULL as verified_pct
        FROM rating_dist
        UNION ALL
        SELECT
            'item_types',
            NULL,
            NULL,
            NULL,
            review_count,
            avg_rating,
            recommend_pct,
            item_type,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL
        FROM item_type_stats
        UNION ALL
        SELECT
            'companies',
            NULL,
            NULL,
            NULL,
            review_count,
            avg_rating,
            NULL,
            NULL,
            company_name,
            NULL,
            NULL,
            NULL,
            NULL
        FROM company_stats WHERE rn <= 10
        UNION ALL
        SELECT
            'travelers',
            NULL,
            NULL,
            NULL,
            count,
            avg_rating,
            NULL,
            NULL,
            NULL,
            traveler_type,
            NULL,
            NULL,
            NULL
        FROM traveler_stats
        UNION ALL
        SELECT
            'sentiment',
            NULL,
            NULL,
            NULL,
            count,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            sentiment,
            NULL,
            NULL
        FROM sentiment_stats
        UNION ALL
        SELECT
            'overall',
            NULL,
            NULL,
            NULL,
            NULL,
            avg_rating,
            recommend_pct,
            NULL,
            NULL,
            NULL,
            NULL,
            total_reviews,
            verified_pct
        FROM overall_stats
        """

        # Execute single combined query
        import time

        # Find a running warehouse
        warehouses = list(w.warehouses.list())
        warehouse_id = None
        for wh in warehouses:
            if wh.state.value == 'RUNNING':
                warehouse_id = wh.id
                break

        if not warehouse_id:
            raise Exception("No running SQL warehouse found")

        # Execute and wait for completion
        result = w.statement_execution.execute_statement(
            warehouse_id=warehouse_id,
            catalog=CATALOG,
            schema=SCHEMA,
            statement=combined_query
        )

        # Wait for completion and get results
        data = w.statement_execution.wait_get_statement_result_chunk_n(
            statement_id=result.statement_id,
            chunk_index=0
        )

        # Parse results
        ratings = []
        item_types = []
        companies = []
        travelers = []
        sentiment = []
        overall = {
            'total_reviews': 0,
            'avg_rating': 0,
            'verified_pct': 0,
            'recommend_pct': 0
        }

        if data and data.data_array:
            for row in data.data_array:
                row_type = row[0]

                if row_type == 'ratings':
                    ratings.append({
                        'rating': int(row[1]) if row[1] else 0,
                        'count': int(row[2]) if row[2] else 0,
                        'avg_helpful': float(row[3]) if row[3] else 0
                    })
                elif row_type == 'item_types':
                    item_types.append({
                        'item_type': row[7],
                        'review_count': int(row[4]) if row[4] else 0,
                        'avg_rating': float(row[5]) if row[5] else 0,
                        'recommend_pct': float(row[6]) if row[6] else 0
                    })
                elif row_type == 'companies':
                    companies.append({
                        'company_name': row[8],
                        'review_count': int(row[4]) if row[4] else 0,
                        'avg_rating': float(row[5]) if row[5] else 0
                    })
                elif row_type == 'travelers':
                    travelers.append({
                        'traveler_type': row[9],
                        'count': int(row[4]) if row[4] else 0,
                        'avg_rating': float(row[5]) if row[5] else 0
                    })
                elif row_type == 'sentiment':
                    sentiment.append({
                        'sentiment': row[10],
                        'count': int(row[4]) if row[4] else 0
                    })
                elif row_type == 'overall':
                    overall = {
                        'total_reviews': int(row[11]) if row[11] else 0,
                        'avg_rating': float(row[5]) if row[5] else 0,
                        'verified_pct': float(row[12]) if row[12] else 0,
                        'recommend_pct': float(row[6]) if row[6] else 0
                    }

        response_data = {
            'ratings': sorted(ratings, key=lambda x: x['rating']),
            'item_types': item_types,
            'companies': companies,
            'travelers': travelers,
            'sentiment': sentiment,
            'overall': overall
        }

        # Cache the result
        _review_stats_cache = response_data
        _review_stats_cache_time = time_module.time()

        print("DEBUG: Review stats fetched and cached successfully")
        response = jsonify(response_data)
        response.headers['Cache-Control'] = 'public, max-age=600'
        return response

    except Exception as e:
        import traceback
        error_details = traceback.format_exc()
        print(f"ERROR: Exception in review stats endpoint: {error_details}")
        app.logger.error(f"Error in review stats endpoint: {str(e)}\n{error_details}")

        # Return mock data as fallback if query fails
        return jsonify({
            'ratings': [
                {'rating': 1, 'count': 50, 'avg_helpful': 2.5},
                {'rating': 2, 'count': 80, 'avg_helpful': 3.2},
                {'rating': 3, 'count': 150, 'avg_helpful': 4.1},
                {'rating': 4, 'count': 350, 'avg_helpful': 5.8},
                {'rating': 5, 'count': 370, 'avg_helpful': 7.2}
            ],
            'item_types': [
                {'item_type': 'Flight', 'review_count': 450, 'avg_rating': 4.2, 'recommend_pct': 78.5},
                {'item_type': 'Hotel', 'review_count': 380, 'avg_rating': 4.3, 'recommend_pct': 82.1},
                {'item_type': 'Package', 'review_count': 170, 'avg_rating': 4.5, 'recommend_pct': 85.3}
            ],
            'companies': [
                {'company_name': 'Delta Air Lines', 'review_count': 85, 'avg_rating': 4.6},
                {'company_name': 'Marriott Hotels', 'review_count': 72, 'avg_rating': 4.5},
                {'company_name': 'United Airlines', 'review_count': 68, 'avg_rating': 4.4},
                {'company_name': 'Hilton Hotels', 'review_count': 65, 'avg_rating': 4.4},
                {'company_name': 'British Airways', 'review_count': 58, 'avg_rating': 4.3}
            ],
            'travelers': [
                {'traveler_type': 'Business', 'count': 320, 'avg_rating': 4.1},
                {'traveler_type': 'Leisure', 'count': 450, 'avg_rating': 4.4},
                {'traveler_type': 'Family', 'count': 230, 'avg_rating': 4.3}
            ],
            'sentiment': [
                {'sentiment': 'Positive', 'count': 720},
                {'sentiment': 'Neutral', 'count': 150},
                {'sentiment': 'Negative', 'count': 130}
            ],
            'overall': {
                'total_reviews': 1000,
                'avg_rating': 4.25,
                'verified_pct': 72.5,
                'recommend_pct': 80.3
            },
            'error': f'Query failed, showing mock data: {str(e)}'
        })


@app.route('/api/hotels/stats', methods=['GET'])
def get_hotel_stats():
    """Get hotel statistics from Unity Catalog synced_hotels table"""
    try:
        # Query cities with highest star ratings
        cities_query = f"""
        SELECT
            city,
            AVG(star_rating) as avg_rating,
            COUNT(*) as count
        FROM {CATALOG}.{SCHEMA}.synced_hotels
        WHERE city IS NOT NULL AND star_rating IS NOT NULL
        GROUP BY city
        ORDER BY avg_rating DESC
        LIMIT 10
        """

        # Query average price by room type
        room_prices_query = f"""
        SELECT
            room_type,
            AVG(total_price) as avg_price,
            COUNT(*) as count
        FROM {CATALOG}.{SCHEMA}.synced_hotels
        WHERE room_type IS NOT NULL AND total_price IS NOT NULL
        GROUP BY room_type
        ORDER BY avg_price DESC
        """

        # Query amenities breakdown
        amenities_query = f"""
        SELECT
            CASE
                WHEN free_breakfast = true AND free_cancellation = true THEN 'Both'
                WHEN free_breakfast = true AND free_cancellation = false THEN 'Breakfast Only'
                WHEN free_breakfast = false AND free_cancellation = true THEN 'Cancellation Only'
                ELSE 'Neither'
            END as type,
            COUNT(*) as count
        FROM {CATALOG}.{SCHEMA}.synced_hotels
        GROUP BY type
        """

        # Query overall statistics
        overall_query = f"""
        SELECT
            COUNT(*) as total_hotels,
            AVG(star_rating) as avg_rating,
            AVG(total_price) as avg_price
        FROM {CATALOG}.{SCHEMA}.synced_hotels
        WHERE star_rating IS NOT NULL AND total_price IS NOT NULL
        """

        # Execute queries using SQL warehouse
        from databricks.sdk.service.sql import StatementState

        # Get warehouse ID from environment variable or find a running one
        warehouse_id = os.environ.get("DATABRICKS_WAREHOUSE_ID")
        if not warehouse_id:
            warehouses = list(w.warehouses.list())
            for wh in warehouses:
                if wh.state.value == 'RUNNING':
                    warehouse_id = wh.id
                    break
            if not warehouse_id:
                raise Exception("No running SQL warehouse found")

        # Execute cities query
        cities_result = w.statement_execution.execute_statement(
            warehouse_id=warehouse_id,
            catalog=CATALOG,
            schema=SCHEMA,
            statement=cities_query
        ).result()

        cities = []
        if cities_result and cities_result.data_array:
            for row in cities_result.data_array:
                cities.append({
                    'city': row[0],
                    'avg_rating': float(row[1]) if row[1] else 0,
                    'count': int(row[2]) if row[2] else 0
                })

        # Execute room prices query
        room_prices_result = w.statement_execution.execute_statement(
            warehouse_id=warehouse_id,
            catalog=CATALOG,
            schema=SCHEMA,
            statement=room_prices_query
        ).result()

        room_prices = []
        if room_prices_result and room_prices_result.data_array:
            for row in room_prices_result.data_array:
                room_prices.append({
                    'room_type': row[0],
                    'avg_price': float(row[1]) if row[1] else 0,
                    'count': int(row[2]) if row[2] else 0
                })

        # Execute amenities query
        amenities_result = w.statement_execution.execute_statement(
            warehouse_id=warehouse_id,
            catalog=CATALOG,
            schema=SCHEMA,
            statement=amenities_query
        ).result()

        amenities = []
        if amenities_result and amenities_result.data_array:
            for row in amenities_result.data_array:
                amenities.append({
                    'type': row[0],
                    'count': int(row[1]) if row[1] else 0
                })

        # Execute overall query
        overall_result = w.statement_execution.execute_statement(
            warehouse_id=warehouse_id,
            catalog=CATALOG,
            schema=SCHEMA,
            statement=overall_query
        ).result()

        overall = {
            'total_hotels': 0,
            'avg_rating': 0,
            'avg_price': 0
        }
        if overall_result and overall_result.data_array and len(overall_result.data_array) > 0:
            row = overall_result.data_array[0]
            overall = {
                'total_hotels': int(row[0]) if row[0] else 0,
                'avg_rating': float(row[1]) if row[1] else 0,
                'avg_price': float(row[2]) if row[2] else 0
            }

        return jsonify({
            'cities': cities,
            'room_prices': room_prices,
            'amenities': amenities,
            'overall': overall
        })

    except Exception as e:
        import traceback
        error_details = traceback.format_exc()
        print(f"ERROR: Exception in hotel stats endpoint: {error_details}")
        app.logger.error(f"Error in hotel stats endpoint: {str(e)}\n{error_details}")

        # Return mock data as fallback if query fails
        return jsonify({
            'cities': [
                {'city': 'Paris', 'avg_rating': 4.8, 'count': 250},
                {'city': 'London', 'avg_rating': 4.7, 'count': 220},
                {'city': 'New York', 'avg_rating': 4.6, 'count': 300},
                {'city': 'Tokyo', 'avg_rating': 4.5, 'count': 180},
                {'city': 'Barcelona', 'avg_rating': 4.4, 'count': 150},
                {'city': 'Amsterdam', 'avg_rating': 4.3, 'count': 140},
                {'city': 'Rome', 'avg_rating': 4.2, 'count': 160},
                {'city': 'Dubai', 'avg_rating': 4.1, 'count': 190},
                {'city': 'Singapore', 'avg_rating': 4.0, 'count': 120},
                {'city': 'Sydney', 'avg_rating': 3.9, 'count': 110}
            ],
            'room_prices': [
                {'room_type': 'Suite', 'avg_price': 450.50, 'count': 120},
                {'room_type': 'Deluxe', 'avg_price': 320.75, 'count': 250},
                {'room_type': 'Standard', 'avg_price': 180.25, 'count': 400},
                {'room_type': 'Economy', 'avg_price': 95.00, 'count': 230}
            ],
            'amenities': [
                {'type': 'Both', 'count': 450},
                {'type': 'Breakfast Only', 'count': 280},
                {'type': 'Cancellation Only', 'count': 190},
                {'type': 'Neither', 'count': 80}
            ],
            'overall': {
                'total_hotels': 1000,
                'avg_rating': 4.3,
                'avg_price': 245.50
            },
            'error': f'Query failed, showing mock data: {str(e)}'
        })


@app.route('/api/chat', methods=['POST'])
def chat():
    """Handle chat messages and interact with the multi-agent supervisor"""
    try:
        data = request.json
        user_message = data.get('message', '')

        if not user_message:
            return jsonify({'error': 'Message is required'}), 400

        # Get or create session ID
        if 'session_id' not in session:
            session['session_id'] = str(uuid.uuid4())

        session_id = session['session_id']

        # Prepare the request payload for the multi-agent supervisor
        system_message = f"""You are an intelligent assistant for Skyscanner Marketplace Intelligence Hub.
You have access to data about Flights, Hotels, Packages, and Customer Reviews.

The data is stored in Databricks Delta tables:
- {CATALOG}.{SCHEMA}.flights
- {CATALOG}.{SCHEMA}.hotels
- {CATALOG}.{SCHEMA}.packages
- {CATALOG}.{SCHEMA}.reviews

You can help users:
1. Search and analyze flight data (airlines, prices, routes, availability)
2. Find and compare hotel information (locations, ratings, amenities, prices)
3. Browse travel packages (destinations, inclusions, pricing, availability)
4. Review customer feedback and sentiment analysis
5. Generate insights and recommendations based on the data

Always be helpful, accurate, and provide data-driven insights when possible.
Use SQL queries against the Delta tables to retrieve relevant information."""

        # Format messages for the endpoint
        messages = [
            {"role": "system", "content": system_message},
            {"role": "user", "content": user_message}
        ]

        # Call the multi-agent supervisor endpoint with correct schema
        payload = {
            "input": messages
        }

        print(f"DEBUG: Sending payload to endpoint {ENDPOINT_NAME}: {payload}")

        response = w.serving_endpoints.query(
            name=ENDPOINT_NAME,
            dataframe_records=[payload]
        )

        print(f"DEBUG: Received response: {response}")
        print(f"DEBUG: Response type: {type(response)}")

        # Extract the assistant's response - try multiple formats
        assistant_message = ""

        # Handle QueryEndpointResponse object directly
        if hasattr(response, 'predictions'):
            raw_predictions = response.predictions
            print(f"DEBUG: Raw predictions attribute: {raw_predictions}")
            print(f"DEBUG: Predictions type: {type(raw_predictions)}")

            # Check if predictions is a dict with 'output' key
            if isinstance(raw_predictions, dict) and 'output' in raw_predictions:
                output = raw_predictions['output']
                print(f"DEBUG: Found output in predictions dict: {output}")

                # Extract message content from the output
                if isinstance(output, list) and len(output) > 0:
                    message_obj = output[0]
                    if isinstance(message_obj, dict) and 'content' in message_obj:
                        content = message_obj['content']
                        if isinstance(content, list) and len(content) > 0:
                            text_obj = content[0]
                            if isinstance(text_obj, dict) and 'text' in text_obj:
                                assistant_message = text_obj['text']
                                print(f"DEBUG: Extracted message from output: {assistant_message[:200]}")

        # If we didn't get the message yet, try the old approach
        if not assistant_message:
            # Convert response to dict if needed
            if hasattr(response, 'as_dict'):
                response_dict = response.as_dict()
            elif hasattr(response, '__dict__'):
                response_dict = response.__dict__
            else:
                response_dict = dict(response) if isinstance(response, dict) else {}

            print(f"DEBUG: Response dict: {response_dict}")

            # Try different response formats
            if isinstance(response_dict, dict):
                # Format 1: Direct predictions list
                if 'predictions' in response_dict and isinstance(response_dict['predictions'], list) and len(response_dict['predictions']) > 0:
                    prediction = response_dict['predictions'][0]
                    print(f"DEBUG: Found predictions list, first item: {prediction}")
                    print(f"DEBUG: Prediction type: {type(prediction)}")

                    # If prediction is a dict, try to extract the message
                    if isinstance(prediction, dict):
                        # Try different nested formats
                        if 'content' in prediction:
                            assistant_message = prediction['content']
                        elif 'text' in prediction:
                            assistant_message = prediction['text']
                        elif 'message' in prediction:
                            if isinstance(prediction['message'], dict):
                                assistant_message = prediction['message'].get('content', str(prediction['message']))
                            else:
                                assistant_message = str(prediction['message'])
                        elif 'output' in prediction:
                            assistant_message = prediction['output']
                        elif 'response' in prediction:
                            assistant_message = prediction['response']
                        else:
                            # Return the full prediction dict as formatted string
                            assistant_message = str(prediction)
                    else:
                        # If prediction is a string, use it directly
                        assistant_message = str(prediction)
                # Format 2: Choices format (OpenAI-style)
                elif 'choices' in response_dict and isinstance(response_dict['choices'], list) and len(response_dict['choices']) > 0:
                    choice = response_dict['choices'][0]
                    if isinstance(choice, dict) and 'message' in choice:
                        assistant_message = choice['message'].get('content', str(choice))
                    else:
                        assistant_message = str(choice)
                # Format 3: Direct content/text field
                elif 'content' in response_dict:
                    assistant_message = response_dict['content']
                elif 'text' in response_dict:
                    assistant_message = response_dict['text']
                else:
                    print(f"DEBUG: Unknown response format")
                    assistant_message = f"Debug: Full response: {str(response_dict)[:1000]}"
            else:
                assistant_message = f"Debug: Response is not a dict: {str(response)[:1000]}"

        print(f"DEBUG: Final assistant message: {assistant_message}")

        return jsonify({
            'response': assistant_message,
            'session_id': session_id
        })

    except Exception as e:
        import traceback
        error_details = traceback.format_exc()
        print(f"ERROR: Exception in chat endpoint: {error_details}")
        app.logger.error(f"Error in chat endpoint: {str(e)}\n{error_details}")
        return jsonify({
            'error': f'An error occurred: {str(e)}',
            'details': error_details
        }), 500


@app.route('/api/clear', methods=['POST'])
def clear_session():
    """Clear the current chat session"""
    session['session_id'] = str(uuid.uuid4())
    return jsonify({'message': 'Session cleared successfully'})


@app.route('/api/stats', methods=['GET'])
def get_stats():
    """Get basic statistics about the data"""
    # Return default stats since Databricks Connect requires cluster configuration
    # which is not available in Databricks Apps serverless environment
    return jsonify({
        'flights': 1000,
        'hotels': 500,
        'packages': 300,
        'reviews': 800
    })


@app.route('/health')
def health():
    """Health check endpoint"""
    return jsonify({'status': 'healthy', 'version': '1.0.0'})


@app.route('/api/test', methods=['POST'])
def test():
    """Test endpoint without calling serving endpoint"""
    try:
        data = request.json
        user_message = data.get('message', '')
        return jsonify({
            'response': f'Test response received your message: {user_message}',
            'session_id': 'test-123'
        })
    except Exception as e:
        import traceback
        return jsonify({
            'error': str(e),
            'details': traceback.format_exc()
        }), 500


@app.route('/api/insights', methods=['POST'])
def get_insights():
    """Generate insights based on filters from the Insights Bot"""
    try:
        data = request.json
        company_name = data.get('company_name', '').strip()
        start_date = data.get('start_date', '')
        end_date = data.get('end_date', '')
        attribute = data.get('attribute', '')

        if not attribute:
            return jsonify({'error': 'Please select an insight attribute'}), 400

        # Parse attribute to get table and column
        parts = attribute.split('.')
        if len(parts) != 2:
            return jsonify({'error': 'Invalid attribute format'}), 400

        table_type, column_name = parts

        # Map table types to actual table names
        table_map = {
            'flights': 'synced_flights',
            'hotels': 'synced_hotels',
            'packages': 'synced_packages',
            'reviews': 'synced_reviews'
        }

        if table_type not in table_map:
            return jsonify({'error': f'Unknown table type: {table_type}'}), 400

        table_name = f"{CATALOG}.{SCHEMA}.{table_map[table_type]}"

        # Get warehouse ID
        warehouse_id = os.environ.get("DATABRICKS_WAREHOUSE_ID")
        if not warehouse_id:
            warehouses = list(w.warehouses.list())
            for wh in warehouses:
                if wh.state.value == 'RUNNING':
                    warehouse_id = wh.id
                    break
            if not warehouse_id:
                return jsonify({'error': 'No running SQL warehouse found'}), 500

        # Build WHERE clause based on filters
        where_conditions = []

        # Company name filter (search in relevant columns based on table)
        if company_name:
            company_columns = {
                'flights': 'airline',
                'hotels': 'hotel_name',
                'packages': 'package_type',
                'reviews': 'company_name'
            }
            company_col = company_columns.get(table_type)
            if company_col:
                where_conditions.append(f"LOWER({company_col}) LIKE LOWER('%{company_name}%')")

        # Date filter (search in relevant date columns based on table)
        if start_date or end_date:
            date_columns = {
                'flights': 'departure_date',
                'hotels': 'check_in_date',
                'packages': 'departure_date',
                'reviews': 'review_date'
            }
            date_col = date_columns.get(table_type)
            if date_col:
                if start_date:
                    where_conditions.append(f"{date_col} >= '{start_date}'")
                if end_date:
                    where_conditions.append(f"{date_col} <= '{end_date}'")

        where_clause = ""
        if where_conditions:
            where_clause = "WHERE " + " AND ".join(where_conditions)

        # Build insights query based on the selected attribute
        # Generate aggregated insights for the selected column
        query = f"""
            SELECT
                {column_name} as attribute_value,
                COUNT(*) as count,
                COUNT(*) * 100.0 / SUM(COUNT(*)) OVER() as percentage
            FROM {table_name}
            {where_clause}
            {"WHERE" if not where_clause else "AND"} {column_name} IS NOT NULL
            GROUP BY {column_name}
            ORDER BY count DESC
            LIMIT 10
        """.replace("WHERE AND", "WHERE")

        # Execute query
        result = w.statement_execution.execute_statement(
            warehouse_id=warehouse_id,
            catalog=CATALOG,
            schema=SCHEMA,
            statement=query
        ).result()

        insights = []
        if result and result.data_array:
            for row in result.data_array:
                insights.append({
                    'value': str(row[0]) if row[0] is not None else 'N/A',
                    'count': int(row[1]) if row[1] else 0,
                    'percentage': round(float(row[2]), 1) if row[2] else 0
                })

        # Get additional statistics
        stats_query = f"""
            SELECT
                COUNT(*) as total_records,
                COUNT(DISTINCT {column_name}) as unique_values
            FROM {table_name}
            {where_clause}
        """

        stats_result = w.statement_execution.execute_statement(
            warehouse_id=warehouse_id,
            catalog=CATALOG,
            schema=SCHEMA,
            statement=stats_query
        ).result()

        total_records = 0
        unique_values = 0
        if stats_result and stats_result.data_array and len(stats_result.data_array) > 0:
            row = stats_result.data_array[0]
            total_records = int(row[0]) if row[0] else 0
            unique_values = int(row[1]) if row[1] else 0

        return jsonify({
            'success': True,
            'attribute': attribute,
            'column_name': column_name,
            'table': table_type,
            'total_records': total_records,
            'unique_values': unique_values,
            'insights': insights,
            'filters': {
                'company_name': company_name,
                'start_date': start_date,
                'end_date': end_date
            }
        })

    except Exception as e:
        import traceback
        error_details = traceback.format_exc()
        print(f"ERROR: Exception in insights endpoint: {error_details}")
        return jsonify({
            'error': f'Failed to generate insights: {str(e)}',
            'details': error_details
        }), 500


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080, debug=False)
