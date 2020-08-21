package javaTest;

public class MinHeap {
	private int[] data;

	public MinHeap(int[] data) {
		this.data = data;
		buildHeap();
	}

	private void buildHeap() {
		//完全二叉树只有数组下标小于或等于 （data.length）/2-1 的元素有孩子结点，遍历这些节点
		for (int i = (data.length) / 2 - 1; i >= 0; i--) {
			//对有孩子节点的元素  heapify
			heapify(i);
		}

	}
	private void heapify(int i) {
		//获取左右节点的数组下标
		int l = left(i);
		int r = right(i);
		// 这是一个临时变量， 表示 跟节点， 左节点， 右节点，中间的最小值的节点小标
		int smallest = i;
		//存在左节点， 且左节点 的值 小于  以上 比较的较小值
		if (l < data.length && data[l] < data[i]) {
			smallest = l;
		}

		//存在右节点， 且右节点的值小于以上比较的较小值,
		if (r < data.length && data[r] < data[smallest]) {
			smallest = r;
		}
		//左右节点的值 都大于根节点，  直接return  , 不做任何操作
		if (i == smallest) {
			return;
		}
		//交换根节点和左右节点中最小的那个值， 把根节点 的值替换下去
		swap(i, smallest);

		// 由于 替换后 左右子树，会被影响， 所以要对影响的子树 在进行heapify
		heapify(smallest);

	}

	//获取右节点的数组下标
	private int right(int i) {
		return (i + 1) << 1;
	}

	//获取做节点的数组下标
	private int left(int i) {
		return ((i + 1) << 1) - 1;
	}

	//交换元素的位置
	private void swap(int i, int j) {
		int tmp = data[i];
		data[i] = data[j];
		data[j] = tmp;
	}

	//获取根元素， 并重新heapify
	public void setRoot(int root) {
		data[0] = root;
		heapify(0);
	}

	// 获取对中的最小的元素，根元素
	public int getRoot() {
		return data[0];
	}


}
