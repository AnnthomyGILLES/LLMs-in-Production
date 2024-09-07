from typing import List, Dict, Any

from qdrant_client import QdrantClient, models


class QdrantHybridQuery:
    def __init__(self, url: str = "http://localhost:6333"):
        self.client = QdrantClient(url=url)

    def hybrid_search(
        self,
        collection_name: str,
        dense_vector: List[float],
        sparse_vector: Dict[int, float],
        fusion_method: str = "rrf",
        limit: int = 10,
    ) -> List[Dict[str, Any]]:
        """
        Perform a hybrid search using both dense and sparse vectors.

        :param collection_name: Name of the Qdrant collection
        :param dense_vector: Dense vector for querying
        :param sparse_vector: Sparse vector for querying (dictionary of {index: value})
        :param fusion_method: Fusion method ('rrf' or 'dbsf')
        :param limit: Number of results to return
        :return: List of search results
        """
        prefetch = [
            models.Prefetch(
                query=models.SparseVector(
                    indices=list(sparse_vector.keys()),
                    values=list(sparse_vector.values()),
                ),
                using="sparse",
                limit=20,
            ),
            models.Prefetch(
                query=dense_vector,
                using="dense",
                limit=20,
            ),
        ]

        fusion = models.Fusion.RRF if fusion_method == "rrf" else models.Fusion.DBSF

        results = self.client.query_points(
            collection_name=collection_name,
            prefetch=prefetch,
            query=models.FusionQuery(fusion=fusion),
            limit=limit,
        )

        return results

    def multi_stage_search(
        self,
        collection_name: str,
        initial_vector: List[float],
        refine_vector: List[float],
        initial_limit: int = 1000,
        final_limit: int = 10,
    ) -> List[Dict[str, Any]]:
        """
        Perform a multi-stage search using different vector representations.

        :param collection_name: Name of the Qdrant collection
        :param initial_vector: Vector for initial search (e.g., MRL byte vector)
        :param refine_vector: Vector for refinement (e.g., full vector)
        :param initial_limit: Number of candidates to fetch in the first stage
        :param final_limit: Number of results to return after refinement
        :return: List of search results
        """
        results = self.client.query_points(
            collection_name=collection_name,
            prefetch=models.Prefetch(
                query=initial_vector,
                using="initial_vector",
                limit=initial_limit,
            ),
            query=refine_vector,
            using="refine_vector",
            limit=final_limit,
        )

        return results

    def query_by_id(
        self,
        collection_name: str,
        point_id: str,
        vector_name: str = None,
        lookup_from: Dict[str, str] = None,
        limit: int = 10,
    ) -> List[Dict[str, Any]]:
        """
        Query points using a vector from an existing point.

        :param collection_name: Name of the Qdrant collection
        :param point_id: ID of the point to use as query
        :param vector_name: Name of the vector to use (if not default)
        :param lookup_from: Dictionary specifying another collection and vector name
        :param limit: Number of results to return
        :return: List of search results
        """
        query_params = {
            "collection_name": collection_name,
            "query": point_id,
            "limit": limit,
        }

        if vector_name:
            query_params["using"] = vector_name

        if lookup_from:
            query_params["lookup_from"] = models.LookupFrom(**lookup_from)

        results = self.client.query_points(**query_params)

        return results

    def rerank_with_payload(
        self,
        collection_name: str,
        query_vector: List[float],
        filters: List[Dict[str, Any]],
        order_by: str,
        limit: int = 10,
    ) -> List[Dict[str, Any]]:
        """
        Rerank search results using payload values.

        :param collection_name: Name of the Qdrant collection
        :param query_vector: Vector for initial search
        :param filters: List of filters to apply
        :param order_by: Field to order results by
        :param limit: Number of results to return
        :return: List of search results
        """
        prefetch = [
            models.Prefetch(
                query=query_vector,
                filter=models.Filter(
                    must=[models.FieldCondition(**f) for f in filters]
                ),
                limit=limit * len(filters),
            )
        ]

        results = self.client.query_points(
            collection_name=collection_name,
            prefetch=prefetch,
            query=models.OrderByQuery(order_by=order_by),
            limit=limit,
        )

        return results

    def grouped_search(
        self,
        collection_name: str,
        query_vector: List[float],
        group_by: str,
        limit: int = 4,
        group_size: int = 2,
    ) -> List[Dict[str, Any]]:
        """
        Perform a grouped search query.

        :param collection_name: Name of the Qdrant collection
        :param query_vector: Vector for search
        :param group_by: Field to group results by
        :param limit: Number of groups to return
        :param group_size: Number of results per group
        :return: List of grouped search results
        """
        results = self.client.query_points_groups(
            collection_name=collection_name,
            query=query_vector,
            group_by=group_by,
            limit=limit,
            group_size=group_size,
        )

        return results
