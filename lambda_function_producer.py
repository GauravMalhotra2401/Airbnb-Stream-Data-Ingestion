import json
import random
from datetime import datetime, timedelta
import boto3

sqs_client = boto3.client("sqs")
sqs_queue_url = "https://sqs.us-east-1.amazonaws.com/058264373160/AirbnbBookingQueue"

locations = [
    "New York City, USA",
    "Paris, France",
    "London, UK",
    "Tokyo, Japan",
    "Los Angeles, USA",
    "Rome, Italy",
    "Barcelona, Spain",
    "Sydney, Australia",
    "Amsterdam, Netherlands",
    "San Francisco, USA",
    "Berlin, Germany",
    "Rio de Janeiro, Brazil",
    "Bangkok, Thailand",
    "Dubai, UAE",
    "Istanbul, Turkey",
    "Toronto, Canada",
    "Singapore, Singapore",
    "Prague, Czech Republic",
    "Hong Kong, China",
    "Florence, Italy",
    "Buenos Aires, Argentina",
    "Miami, USA",
    "Venice, Italy",
    "San Diego, USA",
    "Vienna, Austria",
    "Lisbon, Portugal",
    "Seoul, South Korea",
    "Madrid, Spain",
    "Chicago, USA",
    "Melbourne, Australia",
    "Edinburgh, UK",
    "Vancouver, Canada",
    "Austin, USA",
    "Dublin, Ireland",
    "Copenhagen, Denmark",
    "Budapest, Hungary",
    "Montreal, Canada",
    "Cape Town, South Africa",
    "Munich, Germany",
    "New Orleans, USA",
    "Stockholm, Sweden",
    "Milan, Italy",
    "Kyoto, Japan",
    "Seattle, USA",
    "Marrakesh, Morocco",
    "Oslo, Norway",
    "Reykjavik, Iceland",
    "Helsinki, Finland",
    "Brussels, Belgium",
    "Auckland, New Zealand",
    "Zurich, Switzerland",
    "Porto, Portugal",
    "Krakow, Poland",
    "Glasgow, UK",
    "Lyon, France",
    "Nice, France",
    "Dubrovnik, Croatia",
    "Mexico City, Mexico",
    "Shanghai, China",
    "Moscow, Russia",
    "Ho Chi Minh City, Vietnam",
    "San Juan, Puerto Rico",
    "Charleston, USA",
    "Siem Reap, Cambodia",
    "Phuket, Thailand",
    "Queenstown, New Zealand",
    "Santorini, Greece",
    "Quebec City, Canada",
    "Hoi An, Vietnam",
    "Granada, Spain",
    "Seville, Spain",
    "Cartagena, Colombia",
    "Taipei, Taiwan",
    "Bath, UK",
    "Yangon, Myanmar",
    "Savannah, USA",
    "St. Petersburg, Russia",
    "Napa, USA",
    "Palm Springs, USA",
    "Chiang Mai, Thailand",
    "Asheville, USA"
]

def fetch_start_end_date():
    start_date = datetime.now() + timedelta(days = random.randint(0,365))
    length_of_stay = random.randint(1,14)
    end_date = start_date + timedelta(days = length_of_stay)
    return start_date.strftime("%Y-%m-%d"),end_date.strftime("%Y-%m-%d"),length_of_stay

def calcPrice(length_of_stay):
    price_per_night = random.randint(50,300)
    total_price = price_per_night * length_of_stay
    return total_price

def test_me():
    start_date, end_date, length_of_stay = fetch_start_end_date()
    return {
        "bookingId":random.randint(10000000,99999999),
        "UserId":random.randint(10000000,99999999),
        "propertyId":random.randint(1000000,9999999),
        "location":random.choice(locations),
        "startDate":start_date,
        "endDate":end_date,
        "price": calcPrice(length_of_stay)
    }


def lambda_handler(event, context):
    booking_data = test_me()
    print(booking_data)

    sqs_client.send_message(
        QueueUrl = sqs_queue_url,
        MessageBody = json.dumps(booking_data)
    )

    return {
       "statusCode":200,
       "Body":json.dumps("Message Sent Successfully")
    }