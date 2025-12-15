module.exports = function (eleventyConfig) {
  eleventyConfig.addPassthroughCopy("src/assets");
  eleventyConfig.addPassthroughCopy("src/_data/timeseries");
  eleventyConfig.addPassthroughCopy("src/_data/gauges.json");

  return {
    dir: {
      input: "src",
      output: "dist",
      includes: "_includes",
      data: "_data"
    },
    pathPrefix: "/glofas-anadyr/"
  };
};
