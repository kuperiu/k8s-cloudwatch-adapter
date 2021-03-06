package config

// MetricsDiscoveryConfig describes the configuration for all metrics.
type MetricsDiscoveryConfig struct {
	// Series specify how to discover and map CloudWatch metrics to
	// custom metrics API resources.
	Series []MetricSeriesConfig `yaml:"series"`
}

// MetricSeriesConfig contains the specification for a metric series.
type MetricSeriesConfig struct {
	// Name specifies the series name.
	Name string `yaml:"name"`

	// GroupResource specifies the mapping of this series to a Kubernetes resource.
	Resource GroupResource `yaml:"resource"`

	// Queries specify the CloudWatch metrics query to retrieve data for this series.
	Queries []MetricDataQuery `yaml:"queries"`
}

// GroupResource represents a Kubernetes group-resource.
type GroupResource struct {
	Group    string `yaml:"group,omitempty"`
	Resource string `yaml:"resource"`
}

// MetricDataQuery represents the query structure used in GetMetricData operation to CloudWatch API.
type MetricDataQuery struct {
	// Resources specifies how associated Kubernetes resources should be discovered for
	// the given metrics.
	Resources string `yaml:"resources"`

	// The math expression to be performed on the returned data, if this structure
	// is performing a math expression. For more information about metric math expressions,
	// see Metric Math Syntax and Functions (http://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/using-metric-math.html#metric-math-syntax)
	// in the Amazon CloudWatch User Guide.
	//

	// A short name used to tie this structure to the results in the response. This
	// name must be unique within a single call to GetMetricData. If you are performing
	// math expressions on this set of data, this name represents that data and
	// can serve as a variable in the mathematical expression. The valid characters
	// are letters, numbers, and underscore. The first character must be a lowercase
	// letter.
	//
	// Id is a required field
	ID string `yaml:"id"`

	// A human-readable label for this metric or expression. This is especially
	// useful if this is an expression, so that you know what the value represents.
	// If the metric or expression is shown in a CloudWatch dashboard widget, the
	// label is shown. If Label is omitted, CloudWatch generates a default.
	Label string `yaml:"label"`

	// The metric to be returned, along with statistics, period, and units. Use
	// this parameter only if this structure is performing a data retrieval and
	// not performing a math expression on the returned data.

	Query string `yaml:"query"`
}
