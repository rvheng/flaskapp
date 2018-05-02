from flask import Flask, render_template, flash, redirect, url_for, session, request, logging, jsonify
from flask_mysqldb import MySQL
from wtforms import Form, StringField, TextAreaField, PasswordField, validators
import csv, ast, os

UPLOAD_FOLDER = 'raw_data/uploads'
ALLOWED_EXTENSIONS = set(['csv'])

app = Flask(__name__)

# Config folder for data upload
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
# Config maximum upload to 8 MB
app.config['MAX_CONTENT_LENGTH'] = 8 * 1024 * 1024
# Config MySQL
app.config['MYSQL_HOST'] = 'localhost'
app.config['MYSQL_USER'] = 'root'
app.config['MYSQL_PASSWORD'] = 'password'
app.config['MYSQL_DB'] = 'myflaskapp'
app.config['MYSQL_CURSORCLASS'] = 'DictCursor'

#init MYSQL
mysql = MySQL(app)

@app.route('/')
def index():
        return render_template('home.html')

@app.route('/graph')
def graph(chartID = 'chart_ID', chart_type = 'line', chart_height = 500):

    col = 'PRODVAL_311119B'
    table = 'dataset_asm_product_311119b'
    statement = 'select ' + col + ' from ' + table + ' ;'
    # Create cursor
    cur = mysql.connection.cursor()
    cur.execute(statement)
    results = cur.fetchall()
    data_series = [x[col] for x in results]
    cur.close()
    chart = {"renderTo": chartID, "type": chart_type, "height": chart_height,}
    series = [{"name": col ,"data": data_series}, {"name": col }]
    title = {"text": table}
    xAxis = {"categories": []}
    yAxis = {"title": {"text": table}, "format": '{value:.2f}'}
    return render_template('graph.html', chartID=chartID, chart=chart, series=series, title=title, xAxis=xAxis, yAxis=yAxis)

@app.route('/import')
def importData():
    return render_template('import.html')

@app.route('/upload', methods = ['GET', 'POST'])
def upload():
    if request.method == 'POST':
        # check if the post request has the file part
        if 'file' not in request.files:
            flash('No file part')
            return redirect(request.url)
        file = request.files['file']
        # if user does not select file, browser also
        # submit an empty part without filename
        if file.filename == '':
            flash('No selected file')
            return redirect(request.url)
        if file:
            filename = file.filename
            file.save(os.path.join(app.config['UPLOAD_FOLDER'], filename))
            f_name, f_extension = os.path.splitext(filename)
            populate(f_name)
            return redirect('import')

    return render_template('import.html')

# Gets called on upload
def populate(fname):
    tablename = fname
    filename = app.config['UPLOAD_FOLDER'] + '/' + tablename + '.csv'
    f = open(filename, 'r')
    reader = csv.reader(f)

    # Arrays to store columns and their datatypes
    data, headers, type_list = [], [], []

    clean_data, clean_headers, clean_type = [], [], []

    # We can iterate over the rows in our CSV, call our function above, and populate our lists.
    for row in reader:
        if len(headers) == 0:
            headers = [k.replace(" ", "_") for k in row]
            for col in row:
                type_list.append('')
        else:
            data.append(row)
            for i in range(len(row)):
                # NA is the csv null value
                if type_list[i] == 'varchar' or row[i] == 'NA':
                    pass
                else:
                    var_type = dataType(row[i], type_list[i])
                    type_list[i] = var_type

    f.close()

    safe_col = []
    # Clean the data by only saving the columns that are not strings
    for i in range(len(headers)):
        if type_list[i] != 'varchar':
            safe_col.append(i)

    for col in safe_col:
        clean_headers.append(headers[col].lower())
        clean_type.append(type_list[col].lower())

    clean_data = [[each_list[i] for i in safe_col] for each_list in data]

    # Create cursor
    cur = mysql.connection.cursor()

    ### Region: Create Table based on clean csv header ###
    create_statement = 'create table ' + tablename +' ('

    for i in range(len(clean_headers)):
        create_statement = (create_statement + '{} {}' + ',').format(clean_headers[i], clean_type[i])

    create_statement = create_statement[:-1] + ');'

    # Execute and commit to DB
    cur.execute(create_statement)
    mysql.connection.commit()
    ### EndRegion ###

    ### Region: Insert into table clean (non string) data  ###
    for row in clean_data:
        insert_statement = 'insert into ' + tablename +' ('

        for header in clean_headers:
            insert_statement = (insert_statement + '{}' + ',').format(header)

        insert_statement = insert_statement[:-1] + ') values ('

        for col in row:
            insert_statement = (insert_statement + '{}' + ',').format(col)

        insert_statement = insert_statement[:-1] + ');'

            # Execute and commit to DB
        cur.execute(insert_statement)
        mysql.connection.commit()
    ### EndRegion ###

    # Close Connection
    cur.close()
    return

#  find the data type for each row
def dataType(val, current_type):
    try:
        # Evaluates numbers to an appropriate type, and strings an error
        t = ast.literal_eval(val)
    except ValueError:
        return 'varchar'
    except SyntaxError:
        return 'varchar'

    if type(t) in [int, float]:
        if type(t) is int and current_type not in ['float', 'varchar']:
            # Use smallest possible int type
            if (-32768 < t < 32767) and current_type not in ['int', 'bigint']:
                return 'smallint'
            elif (-2147483648 < t < 2147483647) and current_type not in ['bigint']:
                return 'int'
            else:
                return 'bigint'
        if type(t) is float and current_type not in ['varchar']:
            return 'float(5,2)'
        else:
            return 'varchar'

@app.route('/tables')
def tables():
    cursor = mysql.connection.cursor()
    cursor.execute('select table_name from information_schema.tables where table_schema = "myflaskapp";')
    table_names = [x['table_name'] for x in cursor.fetchall()]

    cursor.execute('select * from gasoline_retail_prices;')
    headers = [i[0] for i in cursor.description]
    results = cursor.fetchall()
    cursor.close()
    # Keep only 20 records
    results = results[0:20]
    return render_template('tables.html', table_names=table_names, headers=headers, results=results)

@app.route('/draw', methods = ['GET', 'POST'])
def draw():
    if request.method == 'POST':
        return redirect('tables')

    return render_template('tables.html')

if __name__ == '__main__':
    app.secret_key='secretkey123'
    app.run(debug=True)
