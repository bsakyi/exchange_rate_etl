provider "aws" {
  region = "us-east-1"
}

resource "aws_s3_bucket" "emil-coinbase-bucket" {
  bucket = "emil-coinbase-bucket"  
  
}


