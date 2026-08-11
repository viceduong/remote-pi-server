# Remote Pi server — multi-stage build
# Stage 1: build TypeScript
FROM node:22-alpine AS build
WORKDIR /app
COPY package.json package-lock.json* ./
RUN npm ci --ignore-scripts || npm install --ignore-scripts
COPY tsconfig.json ./
COPY src ./src
RUN npm run build

# Stage 2: runtime (includes pi itself so the container is self-contained)
FROM node:22-alpine AS runtime
WORKDIR /app
ENV NODE_ENV=production
RUN npm install -g @earendil-works/pi-coding-agent --ignore-scripts || true
COPY package.json ./
RUN npm ci --omit=dev --ignore-scripts || true
COPY --from=build /app/dist ./dist
EXPOSE 8787
ENV REMOTE_PI_WORKDIR=/work
ENV REMOTE_PI_SESSION_DIR=/data/sessions
VOLUME ["/data", "/work"]
CMD ["node", "dist/index.js"]
