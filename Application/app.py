from flask import Flask, render_template, redirect
import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func

from flask import Flask, jsonify

################################################
# Database Setup
#################################################
#Connect to Postgress
rds_connection_string = "carlospazos@127.0.0.1:5432/quotes"
engine = create_engine(f'postgresql://{rds_connection_string}')

# reflect an existing database into a new model
Base = automap_base()
# reflect the tables
Base.prepare(engine, reflect=True)

# Save reference to the table
author = Base.classes.author
quotes = Base.classes.quotes
tag = Base.classes.tag

# Create an instance of Flask
app = Flask(__name__)


# Route to render index.html template using data from Mongo
@app.route("/")
def home():



    # Return template and data
    return render_template("index.html")

@app.route('/quotes')
def quotes_func():

    # Create our session (link) from Python to the DB
    session = Session(engine)

    #Get all precipitation values by date
    text = session.query(quotes.text).all()
    name = session.query(quotes.author_name).all()
    tags = session.query(tag.tag).all()

    #Close session
    session.close()

    

    # Convert list of results into a dictionary
    quote_array = []
    for x in range(0,len(text)):
        
        quote_array.append(

            {
                'text': text[x][0],
                'author name':name[x][0],
                'tags':tags[x][0]


            }


        )

    #Build Output Dictionary
    output_dic = {
        'total': str(len(text)) + " Quotes found",
        'quotes':quote_array
        
    }
    
    return jsonify(output_dic)
        
@app.route('/authors')
def authors_func():

    # Create our session (link) from Python to the DB
    session = Session(engine)

    #Get list of unique authors
    list_authors = session.query(func.distinct(author.author_name)).all()

    details = []
    for x in range(0,len(list_authors)):
        auth_dic={}
        auth_dic['name']=list_authors[x][0],
        auth_dic['description']= session.query(func.distinct(author.description)).filter(author.author_name==list_authors[x][0]).one()[0]
        auth_dic['born']= session.query(func.distinct(author.born)).filter(author.author_name==list_authors[x][0]).one()[0]
        auth_dic['count'] = session.query(func.count(author.author_name)).filter(author.author_name==list_authors[x][0]).one()[0]

        quotesli =[]
        total_quotes_author = session.query(quotes.text,tag.tag).filter(quotes.id==tag.quote_id).filter(quotes.id==author.author_id).filter(author.author_name==list_authors[x][0]).all()
        author_count = session.query(func.count(author.author_name)).filter(author.author_name==list_authors[x][0]).one()[0]
        for y in range(0,author_count):
            quote_dic={}
            quote_dic['text']= total_quotes_author[y][0]
            quote_dic['tags']= total_quotes_author[y][1]
            quotesli.append(quote_dic)
        
        auth_dic['quotes']=quotesli

        details.append(auth_dic)
        

     

    #Close session
    session.close()

    return jsonify(details)

if __name__ == "__main__":
    app.run(debug=True)