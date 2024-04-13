using System.Collections.Generic;

namespace Omegacorp.Core.Model.Utilities
{
    public class Pagination<T>
    {
        public IEnumerable<T> Data { get; set; }
        public int? Rows { get; set; }
    }

    public class PaginationFilter
    {
        public int? Page { get; set; }
        public int? RowsPerPage { get; set; }
        public string SortBy { get; set; } = "1";
        public bool Descending { get; set; } = false;
        public static PaginationFilter FromParameters(int? page = null, int? rowsPerPage = null, string sortBy = "1", bool descending = false)
        {
            PaginationFilter pf = null;
            rowsPerPage = rowsPerPage == 0 ? null : rowsPerPage;
            pf = new PaginationFilter { Page = page, RowsPerPage = rowsPerPage, SortBy = sortBy, Descending = descending };
            return pf;
        }
    }
}