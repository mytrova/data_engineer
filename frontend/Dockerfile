FROM node:20-alpine

WORKDIR /app

# No build step; serve static files with a tiny server
RUN npm i -g http-server

COPY frontend /app

EXPOSE 3000

CMD ["http-server", ".", "-p", "3000", "-c-1"]


