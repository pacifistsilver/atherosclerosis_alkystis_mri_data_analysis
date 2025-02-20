# Use an official Python runtime as the base image
FROM python:3.9-slim

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file into the container
COPY requirements.txt .
COPY compile_rois.py .
COPY config.yml .
# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the current directory contents into the container
COPY . .

# Set environment variables (if needed)
ENV DATA_PATH=/app/rabbit_roi_data
ENV OUTPUT_PATH=/app/output
ENV PYTHONUNBUFFERED=1

# Create directories for data and output
RUN mkdir -p /app/data /app/output

# Run the script when the container launches
CMD ["python", "compile_rois.py"]