import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  // output: 'standalone' produces a self-contained build with only the files
  // needed to run the app — significantly reduces Vercel bundle size and cold-start time.
  output: "standalone",
};

export default nextConfig;
