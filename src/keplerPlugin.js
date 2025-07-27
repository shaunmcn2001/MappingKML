import {injectComponents} from 'kepler.gl';
import QuerySearchPanel from './QuerySearchPanel';

const plugin = {
  SidePanelFactory: {
    components: {
      querySearch: {
        id: 'querySearch',
        label: 'Query Search',
        component: QuerySearchPanel
      }
    }
  }
};

export default plugin;
