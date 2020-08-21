package javaTest;

public enum ProjectType {
	ADS_S(421, "s"), ADS_C(421, "c"), YGPET_S(422, "s"), YGPET_C(422, "c"), YGHEV_C(423, "c"), YGHEV_S(423, "s");
	Integer projectId;
	String type;

	ProjectType(Integer projectId, String type) {
		this.projectId = projectId;
		this.type = type;
	}

	public Integer getProjectId() {
		return projectId;
	}

	public void setProjectId(Integer projectId) {
		this.projectId = projectId;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}
}
